/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.naming.consistency.persistent.raft;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.exception.runtime.NacosRuntimeException;
import com.alibaba.nacos.common.http.Callback;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.model.RestResult;
import com.alibaba.nacos.common.notify.EventPublisher;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
import com.alibaba.nacos.common.utils.IPUtil;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.consistency.DataOperation;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.consistency.ValueChangeEvent;
import com.alibaba.nacos.naming.consistency.persistent.ClusterVersionJudgement;
import com.alibaba.nacos.naming.consistency.persistent.PersistentNotifier;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.*;
import com.alibaba.nacos.naming.monitor.MetricsMonitor;
import com.alibaba.nacos.naming.pojo.Record;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.context.annotation.DependsOn;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPOutputStream;

/**
 * Raft core code.
 *
 * @author nacos
 * @deprecated will remove in 1.4.x
 */
@Deprecated
@DependsOn("ProtocolManager")
@Component
public class RaftCore implements Closeable {

    public static final String API_VOTE = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/vote";

    public static final String API_BEAT = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/beat";

    public static final String API_PUB = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum";

    public static final String API_DEL = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum";

    public static final String API_GET = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum";

    public static final String API_ON_PUB = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum/commit";

    public static final String API_ON_DEL = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum/commit";

    public static final String API_GET_PEER = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/peer";

    public static final Lock OPERATE_LOCK = new ReentrantLock();

    public static final int PUBLISH_TERM_INCREASE_COUNT = 100;

    private volatile ConcurrentMap<String, List<RecordListener>> listeners = new ConcurrentHashMap<>();

    private volatile ConcurrentMap<String, Datum> datums = new ConcurrentHashMap<>();

    private RaftPeerSet peers;

    private final SwitchDomain switchDomain;

    private final GlobalConfig globalConfig;

    private final RaftProxy raftProxy;

    private final RaftStore raftStore;

    private final ClusterVersionJudgement versionJudgement;

    public final PersistentNotifier notifier;

    private final EventPublisher publisher;

    private final RaftListener raftListener;

    private boolean initialized = false;

    private volatile boolean stopWork = false;

    private ScheduledFuture masterTask = null;

    private ScheduledFuture heartbeatTask = null;

    public RaftCore(RaftPeerSet peers, SwitchDomain switchDomain, GlobalConfig globalConfig, RaftProxy raftProxy,
                    RaftStore raftStore, ClusterVersionJudgement versionJudgement, RaftListener raftListener) {
        this.peers = peers;
        this.switchDomain = switchDomain;
        this.globalConfig = globalConfig;
        this.raftProxy = raftProxy;
        this.raftStore = raftStore;
        this.versionJudgement = versionJudgement;
        this.notifier = new PersistentNotifier(key -> null == getDatum(key) ? null : getDatum(key).value);
        this.publisher = NotifyCenter.registerToPublisher(ValueChangeEvent.class, 16384);
        this.raftListener = raftListener;
    }

    /**
     * Init raft core.
     *
     * @throws Exception any exception during init
     */
    @PostConstruct
    public void init() throws Exception {
        Loggers.RAFT.info("initializing Raft sub-system");
        final long start = System.currentTimeMillis();
        raftStore.loadDatums(notifier, datums); // 加载持久化数据
        setTerm(NumberUtils.toLong(raftStore.loadMeta().getProperty("term"), 0L)); // 加载{nacos_home}\data\naming\meta.properties文件中的周期
        Loggers.RAFT.info("cache loaded, datum count: {}, current term: {}", datums.size(), peers.getTerm());
        initialized = true;
        Loggers.RAFT.info("finish to load data from disk, cost: {} ms.", (System.currentTimeMillis() - start));
        masterTask = GlobalExecutor.registerMasterElection(new MasterElection()); // 定期执行集群leader选举的MasterElection任务
        heartbeatTask = GlobalExecutor.registerHeartbeat(new HeartBeat()); // 定时执行集群节点心跳检测的HeartBeat任务
        versionJudgement.registerObserver(isAllNewVersion -> {
            stopWork = isAllNewVersion;
            if (stopWork) {
                try {
                    shutdown();
                    raftListener.removeOldRaftMetadata();
                } catch (NacosException e) {
                    throw new NacosRuntimeException(NacosException.SERVER_ERROR, e);
                }
            }
        }, 100);
        NotifyCenter.registerSubscriber(notifier); // 注册PersistentNotifier订阅者
        Loggers.RAFT.info("timer started: leader timeout ms: {}, heart-beat timeout ms: {}", GlobalExecutor.LEADER_TIMEOUT_MS, GlobalExecutor.HEARTBEAT_INTERVAL_MS);
    }

    public Map<String, ConcurrentHashSet<RecordListener>> getListeners() {
        return notifier.getListeners();
    }

    /**
     * Signal publish new record. If not leader, signal to leader. If leader, try to commit publish.
     *
     * @param key   key
     * @param value value
     * @throws Exception any exception during publish
     */
    public void signalPublish(String key, Record value) throws Exception {
        if (stopWork) {
            throw new IllegalStateException("old raft protocol already stop work");
        }
        if (!isLeader()) { // 若本节点不是leader节点则将注册请求转发到集群的leader节点
            ObjectNode params = JacksonUtils.createEmptyJsonNode();
            params.put("key", key);
            params.replace("value", JacksonUtils.transferToJsonNode(value));
            Map<String, String> parameters = new HashMap<>(1);
            parameters.put("key", key);
            final RaftPeer leader = getLeader();
            raftProxy.proxyPostLarge(leader.ip, API_PUB, params.toString(), parameters); // 将数据发送到leader节点的/v1/ns/raft/datum接口
            return;
        }
        OPERATE_LOCK.lock();
        try {
            final long start = System.currentTimeMillis();
            final Datum datum = new Datum();
            datum.key = key;
            datum.value = value;
            if (getDatum(key) == null) { // 若key对应的数据不存在
                datum.timestamp.set(1L); // 将更新版本设置为1
            } else { // 若key已存在，则将已有版本加一
                datum.timestamp.set(getDatum(key).timestamp.incrementAndGet());
            }
            ObjectNode json = JacksonUtils.createEmptyJsonNode();
            json.replace("datum", JacksonUtils.transferToJsonNode(datum)); // 需要更新的数据
            json.replace("source", JacksonUtils.transferToJsonNode(peers.local())); // leader节点信息
            onPublish(datum, peers.local());
            final String content = json.toString();
            // 利用CountDownLatch实现一个简单的raft协议写入数据的逻辑，必须集群半数以上节点写入成功才会给客户端返回成功
            final CountDownLatch latch = new CountDownLatch(peers.majorityCount());
            for (final String server : peers.allServersIncludeMyself()) {
                if (isLeader(server)) { // 若是leader直接减一，因为上面已更新了数据
                    latch.countDown();
                    continue;
                }
                final String url = buildUrl(server, API_ON_PUB); // 调用/raft/datum/commit接口同步数据给其他从节点
                HttpClient.asyncHttpPostLarge(url, Arrays.asList("key", key), content, new Callback<String>() {
                    @Override
                    public void onReceive(RestResult<String> result) {
                        if (!result.ok()) {
                            Loggers.RAFT.warn("[RAFT] failed to publish data to peer, datumId={}, peer={}, http code={}", datum.key, server, result.getCode());
                            return;
                        }
                        latch.countDown();
                    }
                    @Override
                    public void onError(Throwable throwable) {
                        Loggers.RAFT.error("[RAFT] failed to publish data to peer", throwable);
                    }
                    @Override
                    public void onCancel() {
                    }
                });
            }
            if (!latch.await(UtilsAndCommons.RAFT_PUBLISH_TIMEOUT, TimeUnit.MILLISECONDS)) {
                // only majority servers return success can we consider this update success
                Loggers.RAFT.error("data publish failed, caused failed to notify majority, key={}", key);
                throw new IllegalStateException("data publish failed, caused failed to notify majority, key=" + key);
            }
            long end = System.currentTimeMillis();
            Loggers.RAFT.info("signalPublish cost {} ms, key: {}", (end - start), key);
        } finally {
            OPERATE_LOCK.unlock();
        }
    }

    /**
     * Signal delete record. If not leader, signal leader delete. If leader, try to commit delete.
     *
     * @param key key
     * @throws Exception any exception during delete
     */
    public void signalDelete(final String key) throws Exception {
        if (stopWork) {
            throw new IllegalStateException("old raft protocol already stop work");
        }
        OPERATE_LOCK.lock();
        try {

            if (!isLeader()) {
                Map<String, String> params = new HashMap<>(1);
                params.put("key", URLEncoder.encode(key, "UTF-8"));
                raftProxy.proxy(getLeader().ip, API_DEL, params, HttpMethod.DELETE);
                return;
            }

            // construct datum:
            Datum datum = new Datum();
            datum.key = key;
            ObjectNode json = JacksonUtils.createEmptyJsonNode();
            json.replace("datum", JacksonUtils.transferToJsonNode(datum));
            json.replace("source", JacksonUtils.transferToJsonNode(peers.local()));

            onDelete(datum.key, peers.local());

            for (final String server : peers.allServersWithoutMySelf()) {
                String url = buildUrl(server, API_ON_DEL);
                HttpClient.asyncHttpDeleteLarge(url, null, json.toString(), new Callback<String>() {
                    @Override
                    public void onReceive(RestResult<String> result) {
                        if (!result.ok()) {
                            Loggers.RAFT
                                .warn("[RAFT] failed to delete data from peer, datumId={}, peer={}, http code={}",
                                    key, server, result.getCode());
                            return;
                        }

                        RaftPeer local = peers.local();

                        local.resetLeaderDue();
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        Loggers.RAFT.error("[RAFT] failed to delete data from peer", throwable);
                    }

                    @Override
                    public void onCancel() {

                    }
                });
            }
        } finally {
            OPERATE_LOCK.unlock();
        }
    }

    /**
     * Do publish. If leader, commit publish to store. If not leader, stop publish because should signal to leader.
     *
     * @param datum  datum
     * @param source source raft peer
     * @throws Exception any exception during publish
     */
    public void onPublish(Datum datum, RaftPeer source) throws Exception {
        if (stopWork) {
            throw new IllegalStateException("old raft protocol already stop work");
        }
        RaftPeer local = peers.local();
        if (datum.value == null) { // 若接收数据为null抛出异常
            Loggers.RAFT.warn("received empty datum");
            throw new IllegalStateException("received empty datum");
        }
        if (!peers.isLeader(source.ip)) { // 非leader节点这里直接抛出异常
            Loggers.RAFT.warn("peer {} tried to publish data but wasn't leader, leader: {}", JacksonUtils.toJson(source), JacksonUtils.toJson(getLeader()));
            throw new IllegalStateException("peer(" + source.ip + ") tried to publish " + "data but wasn't leader");
        }
        if (source.term.get() < local.term.get()) { // 若发布超时则直接抛出异常
            Loggers.RAFT.warn("out of date publish, pub-term: {}, cur-term: {}", JacksonUtils.toJson(source), JacksonUtils.toJson(local));
            throw new IllegalStateException("out of date publish, pub-term:" + source.term.get() + ", cur-term: " + local.term.get());
        }
        local.resetLeaderDue(); // 将leaderDueMs重置为15s + (0到5秒的随机时间) = 15s - 20s
        // if data should be persisted, usually this is true:
        if (KeyBuilder.matchPersistentKey(datum.key)) { // 若key不是以com.alibaba.nacos.naming.iplist.ephemeral.开头的数据数据
            raftStore.write(datum); // 同步写实例数据到文件
        }
        datums.put(datum.key, datum); // 更新缓存中的数据
        if (isLeader()) { // 若本机是leader节点
            local.term.addAndGet(PUBLISH_TERM_INCREASE_COUNT); // 将leader的选举周期加100
        } else {
            if (local.term.get() + PUBLISH_TERM_INCREASE_COUNT > source.term.get()) {
                //set leader term:
                getLeader().term.set(source.term.get());
                local.term.set(getLeader().term.get());
            } else { // 若本机不是的选举周期+100小于leader的选举周期
                local.term.addAndGet(PUBLISH_TERM_INCREASE_COUNT); // 本机选举周期加100
            }
        }
        raftStore.updateTerm(local.term.get()); // 更新缓存文件meta.properties中选举周期
        // 最终订阅者PersistentNotifier会收到该事件变更执行onEvent方法异步更新注册表
        NotifyCenter.publishEvent(ValueChangeEvent.builder().key(datum.key).action(DataOperation.CHANGE).build());
        Loggers.RAFT.info("data added/updated, key={}, term={}", datum.key, local.term);
    }

    /**
     * Do delete. If leader, commit delete to store. If not leader, stop delete because should signal to leader.
     *
     * @param datumKey datum key
     * @param source   source raft peer
     * @throws Exception any exception during delete
     */
    public void onDelete(String datumKey, RaftPeer source) throws Exception {
        if (stopWork) {
            throw new IllegalStateException("old raft protocol already stop work");
        }
        RaftPeer local = peers.local();
        if (!peers.isLeader(source.ip)) {
            Loggers.RAFT.warn("peer {} tried to publish data but wasn't leader, leader: {}", JacksonUtils.toJson(source), JacksonUtils.toJson(getLeader()));
            throw new IllegalStateException("peer(" + source.ip + ") tried to publish data but wasn't leader");
        }
        if (source.term.get() < local.term.get()) {
            Loggers.RAFT.warn("out of date publish, pub-term: {}, cur-term: {}", JacksonUtils.toJson(source), JacksonUtils.toJson(local));
            throw new IllegalStateException("out of date publish, pub-term:" + source.term + ", cur-term: " + local.term);
        }
        local.resetLeaderDue();
        // do apply
        String key = datumKey;
        deleteDatum(key);
        if (KeyBuilder.matchServiceMetaKey(key)) {
            if (local.term.get() + PUBLISH_TERM_INCREASE_COUNT > source.term.get()) {
                //set leader term:
                getLeader().term.set(source.term.get());
                local.term.set(getLeader().term.get());
            } else {
                local.term.addAndGet(PUBLISH_TERM_INCREASE_COUNT);
            }
            raftStore.updateTerm(local.term.get());
        }
        Loggers.RAFT.info("data removed, key={}, term={}", datumKey, local.term);
    }

    @Override
    public void shutdown() throws NacosException {
        this.stopWork = true;
        this.raftStore.shutdown();
        this.peers.shutdown();
        Loggers.RAFT.warn("start to close old raft protocol!!!");
        Loggers.RAFT.warn("stop old raft protocol task for notifier");
        NotifyCenter.deregisterSubscriber(notifier);
        Loggers.RAFT.warn("stop old raft protocol task for master task");
        masterTask.cancel(true);
        Loggers.RAFT.warn("stop old raft protocol task for heartbeat task");
        heartbeatTask.cancel(true);
        Loggers.RAFT.warn("clean old cache datum for old raft");
        datums.clear();
    }

    public class MasterElection implements Runnable {
        @Override
        public void run() {
            try {
                if (stopWork) {
                    return;
                }
                if (!peers.isReady()) { // RaftPeerSet初始化方法中changePeers完成peers初始化将ready置为true
                    return;
                }
                RaftPeer local = peers.local(); // 获取本机对应的RaftPeer
                local.leaderDueMs -= GlobalExecutor.TICK_PERIOD_MS; // leaderDueMs默认是从0到15s随机一个时间，减去500ms
                if (local.leaderDueMs > 0) { // 若leaderDueMs>0继续等待下次执行
                    return;
                }
                local.resetLeaderDue(); // 将leaderDueMs重置为15s + (0到5秒的随机时间) = 15s - 20s
                local.resetHeartbeatDue(); // 将heartbeatDueMs重置为5s
                sendVote(); // 发起集群leader投票
            } catch (Exception e) {
                Loggers.RAFT.warn("[RAFT] error while master election {}", e);
            }
        }

        private void sendVote() {
            RaftPeer local = peers.get(NetUtils.localServer()); // 获取本机对应的RaftPeer
            Loggers.RAFT.info("leader timeout, start voting,leader: {}, term: {}", JacksonUtils.toJson(getLeader()), local.term);
            peers.reset(); // 将leader和所有的RaftPeer的voteFor字段置null
            local.term.incrementAndGet(); // 将本机选举周期加一
            local.voteFor = local.ip; // 将本机的voteFor设置为本机IP
            local.state = RaftPeer.State.CANDIDATE; // 将本机状态由FOLLOWER变更为CANDIDATE状态
            Map<String, String> params = new HashMap<>(1);
            params.put("vote", JacksonUtils.toJson(local));
            for (final String server : peers.allServersWithoutMySelf()) { // 给所有其他成员发送投票请求
                final String url = buildUrl(server, API_VOTE); // 目标服务成员的/raft/vote接口
                try {
                    HttpClient.asyncHttpPost(url, null, params, new Callback<String>() {
                        @Override
                        public void onReceive(RestResult<String> result) {
                            if (!result.ok()) {
                                Loggers.RAFT.error("NACOS-RAFT vote failed: {}, url: {}", result.getCode(), url);
                                return;
                            }
                            RaftPeer peer = JacksonUtils.toObj(result.getData(), RaftPeer.class);
                            Loggers.RAFT.info("received approve from peer: {}", JacksonUtils.toJson(peer));
                            peers.decideLeader(peer); // 计算并决定哪个节点是领导者，若有新的peer超过半数投票，则将leader更换为新peer
                        }
                        @Override
                        public void onError(Throwable throwable) {
                            Loggers.RAFT.error("error while sending vote to server: {}", server, throwable);
                        }
                        @Override
                        public void onCancel() {
                        }
                    });
                } catch (Exception e) {
                    Loggers.RAFT.warn("error while sending vote to server: {}", server);
                }
            }
        }
    }

    /**
     * Received vote.
     *
     * @param remote remote raft peer of vote information
     * @return self-peer information
     */
    public synchronized RaftPeer receivedVote(RaftPeer remote) {
        if (stopWork) {
            throw new IllegalStateException("old raft protocol already stop work");
        }
        if (!peers.contains(remote)) {
            throw new IllegalStateException("can not find peer: " + remote.ip);
        }
        RaftPeer local = peers.get(NetUtils.localServer());
        if (remote.term.get() <= local.term.get()) { // 若remote节点的选举周期小于或等于本机节点的选举周期
            String msg = "received illegitimate vote" + ", voter-term:" + remote.term + ", votee-term:" + local.term;
            Loggers.RAFT.info(msg);
            if (StringUtils.isEmpty(local.voteFor)) {
                local.voteFor = local.ip;  // 若本机的选票为空，则将选票投给自己
            }
            return local;
        }
        local.resetLeaderDue(); // 将leaderDueMs重置为15s + (0到5秒的随机时间) = 15s - 20s
        local.state = RaftPeer.State.FOLLOWER; // 将本机节点状态置为FOLLOWER
        local.voteFor = remote.ip; // 将本机的选票投给remote节点
        local.term.set(remote.term.get()); // 将本机的选举周期设置为remote节点的选举周期
        Loggers.RAFT.info("vote {} as leader, term: {}", remote.ip, remote.term);
        return local;
    }

    public class HeartBeat implements Runnable {
        @Override
        public void run() {
            try {
                if (stopWork) {
                    return;
                }
                if (!peers.isReady()) {  // RaftPeerSet初始化方法中changePeers完成peers初始化将ready置为true
                    return;
                }
                RaftPeer local = peers.local(); // 获取本机对应的RaftPeer
                local.heartbeatDueMs -= GlobalExecutor.TICK_PERIOD_MS; // heartbeatDueMs默认是从0到5s随机一个时间，减去500ms
                if (local.heartbeatDueMs > 0) { // 若leaderDueMs>0继续等待下次执行
                    return;
                }
                local.resetHeartbeatDue(); // 将heartbeatDueMs重置为5s
                sendBeat();
            } catch (Exception e) {
                Loggers.RAFT.warn("[RAFT] error while sending beat {}", e);
            }
        }

        private void sendBeat() throws IOException, InterruptedException {
            RaftPeer local = peers.local();
            if (EnvUtil.getStandaloneMode() || local.state != RaftPeer.State.LEADER) {
                return; // 若是Standalone模式启动或本机不是leader则不发送心跳检查
            }
            if (Loggers.RAFT.isDebugEnabled()) {
                Loggers.RAFT.debug("[RAFT] send beat with {} keys.", datums.size());
            }
            local.resetLeaderDue(); // 将leaderDueMs重置为15s + (0到5秒的随机时间) = 15s - 20s
            // build data
            ObjectNode packet = JacksonUtils.createEmptyJsonNode();
            packet.replace("peer", JacksonUtils.transferToJsonNode(local));
            ArrayNode array = JacksonUtils.createEmptyArrayNode();
            if (switchDomain.isSendBeatOnly()) {
                Loggers.RAFT.info("[SEND-BEAT-ONLY] {}", switchDomain.isSendBeatOnly());
            }
            if (!switchDomain.isSendBeatOnly()) {
                for (Datum datum : datums.values()) { // 将KEY和对应的版本放入element中，最终添加到array中
                    ObjectNode element = JacksonUtils.createEmptyJsonNode();
                    if (KeyBuilder.matchServiceMetaKey(datum.key)) { // key以com.alibaba.nacos.naming.domains.meta.或meta.开头
                        element.put("key", KeyBuilder.briefServiceMetaKey(datum.key));
                    } else if (KeyBuilder.matchInstanceListKey(datum.key)) { // key以com.alibaba.nacos.naming.iplist.或iplist.开头
                        element.put("key", KeyBuilder.briefInstanceListkey(datum.key));
                    }
                    element.put("timestamp", datum.timestamp.get());
                    array.add(element);
                }
            }
            packet.replace("datums", array); // 将array放入数据包
            Map<String, String> params = new HashMap<String, String>(1); // broadcast
            params.put("beat", JacksonUtils.toJson(packet));
            String content = JacksonUtils.toJson(params);
            ByteArrayOutputStream out = new ByteArrayOutputStream(); // 使用GZIP将数据进行压缩
            GZIPOutputStream gzip = new GZIPOutputStream(out);
            gzip.write(content.getBytes(StandardCharsets.UTF_8));
            gzip.close();
            byte[] compressedBytes = out.toByteArray();
            String compressedContent = new String(compressedBytes, StandardCharsets.UTF_8);
            if (Loggers.RAFT.isDebugEnabled()) {
                Loggers.RAFT.debug("raw beat data size: {}, size of compressed data: {}", content.length(), compressedContent.length());
            }
            for (final String server : peers.allServersWithoutMySelf()) { // 遍历每一个从节点（除开自己，自己是leader）
                try {
                    final String url = buildUrl(server, API_BEAT); // 调用从节点的/raft/beat接口发送心跳数据
                    if (Loggers.RAFT.isDebugEnabled()) {
                        Loggers.RAFT.debug("send beat to server " + server);
                    }
                    HttpClient.asyncHttpPostLarge(url, null, compressedBytes, new Callback<String>() {
                        @Override
                        public void onReceive(RestResult<String> result) {
                            if (!result.ok()) { // 若发送失败
                                Loggers.RAFT.error("NACOS-RAFT beat failed: {}, peer: {}", result.getCode(), server);
                                MetricsMonitor.getLeaderSendBeatFailedException().increment();
                                return;
                            }
                            peers.update(JacksonUtils.toObj(result.getData(), RaftPeer.class)); // 更新对应IP的RaftPeer
                            if (Loggers.RAFT.isDebugEnabled()) {
                                Loggers.RAFT.debug("receive beat response from: {}", url);
                            }
                        }
                        @Override
                        public void onError(Throwable throwable) {
                            Loggers.RAFT.error("NACOS-RAFT error while sending heart-beat to peer: {} {}", server, throwable);
                            MetricsMonitor.getLeaderSendBeatFailedException().increment();
                        }
                        @Override
                        public void onCancel() {
                        }
                    });
                } catch (Exception e) {
                    Loggers.RAFT.error("error while sending heart-beat to peer: {} {}", server, e);
                    MetricsMonitor.getLeaderSendBeatFailedException().increment();
                }
            }
        }
    }

    /**
     * Received beat from leader. // TODO split method to multiple smaller method.
     *
     * @param beat beat information from leader
     * @return self-peer information
     * @throws Exception any exception during handle
     */
    public RaftPeer receivedBeat(JsonNode beat) throws Exception {
        if (stopWork) {
            throw new IllegalStateException("old raft protocol already stop work");
        }
        final RaftPeer local = peers.local();
        final RaftPeer remote = new RaftPeer();
        JsonNode peer = beat.get("peer"); // 存放的leader的RaftPeer数据
        remote.ip = peer.get("ip").asText(); // leader的ip
        remote.state = RaftPeer.State.valueOf(peer.get("state").asText());  // leader的state
        remote.term.set(peer.get("term").asLong());  // leader的选举周期
        remote.heartbeatDueMs = peer.get("heartbeatDueMs").asLong();
        remote.leaderDueMs = peer.get("leaderDueMs").asLong();
        remote.voteFor = peer.get("voteFor").asText();
        if (remote.state != RaftPeer.State.LEADER) { // 若接收到的不是leader发送的心跳数据直接抛出异常
            Loggers.RAFT.info("[RAFT] invalid state from master, state: {}, remote peer: {}", remote.state, JacksonUtils.toJson(remote));
            throw new IllegalArgumentException("invalid state from master, state: " + remote.state);
        }
        if (local.term.get() > remote.term.get()) { // 若本机的选举周期大于leader节点的选举周期直接抛出异常
            Loggers.RAFT.info("[RAFT] out of date beat, beat-from-term: {}, beat-to-term: {}, remote peer: {}, and leaderDueMs: {}", remote.term.get(), local.term.get(), JacksonUtils.toJson(remote), local.leaderDueMs);
            throw new IllegalArgumentException("out of date beat, beat-from-term: " + remote.term.get() + ", beat-to-term: " + local.term.get());
        }
        if (local.state != RaftPeer.State.FOLLOWER) { // 若本机的状态不是FOLLOWER，则将其置为FOLLOWER并将选票投给当前的leader节点
            Loggers.RAFT.info("[RAFT] make remote as leader, remote peer: {}", JacksonUtils.toJson(remote));
            local.state = RaftPeer.State.FOLLOWER; // mk follower
            local.voteFor = remote.ip;
        }
        final JsonNode beatDatums = beat.get("datums");
        local.resetLeaderDue(); // 将leaderDueMs重置为15s + (0到5秒的随机时间) = 15s - 20s
        local.resetHeartbeatDue(); // 将heartbeatDueMs重置为5s
        peers.makeLeader(remote); // 更新leader信息，将remote设置为新leader，更新原有leader的节点信息
        if (!switchDomain.isSendBeatOnly()) {
            Map<String, Integer> receivedKeysMap = new HashMap<>(datums.size());
            for (Map.Entry<String, Datum> entry : datums.entrySet()) {
                receivedKeysMap.put(entry.getKey(), 0); // 将当前节点的可以放到一个map中，value都置为0
            }
            List<String> batch = new ArrayList<>(); // now check datums
            int processedCount = 0;
            if (Loggers.RAFT.isDebugEnabled()) {
                Loggers.RAFT.debug("[RAFT] received beat with {} keys, RaftCore.datums' size is {}, remote server: {}, term: {}, local term: {}", beatDatums.size(), datums.size(), remote.ip, remote.term, local.term);
            }
            for (Object object : beatDatums) { // 遍历心跳数据
                processedCount = processedCount + 1;
                JsonNode entry = (JsonNode) object;
                String key = entry.get("key").asText();
                final String datumKey;
                if (KeyBuilder.matchServiceMetaKey(key)) { // key以com.alibaba.nacos.naming.domains.meta.或meta.开头
                    datumKey = KeyBuilder.detailServiceMetaKey(key);
                } else if (KeyBuilder.matchInstanceListKey(key)) { // key以com.alibaba.nacos.naming.iplist.或iplist.开头
                    datumKey = KeyBuilder.detailInstanceListkey(key);
                } else {
                    continue; // ignore corrupted key:
                }
                long timestamp = entry.get("timestamp").asLong();
                receivedKeysMap.put(datumKey, 1); // 将心跳数据覆盖receivedKeysMap中的数据，且其值置为1
                try {
                    if (datums.containsKey(datumKey) && datums.get(datumKey).timestamp.get() >= timestamp && processedCount < beatDatums.size()) {
                        continue; // 若收到的心跳数据在本地存在，且本地的版本大于等于收到的版本，且还有数据未处理完，则直接continue跳过
                    }
                    if (!(datums.containsKey(datumKey) && datums.get(datumKey).timestamp.get() >= timestamp)) {
                        batch.add(datumKey); // 若收到的key在本地没有，或本地版本小于或等于收到的版本，放入批量处理
                    }
                    if (batch.size() < 50 && processedCount < beatDatums.size()) {
                        continue; // 只有batch的数量超过50或接收的数据已处理完毕，才进行获取数据操作
                    }
                    String keys = StringUtils.join(batch, ",");
                    if (batch.size() <= 0) { // 若没有数据需要处理直接跳过
                        continue;
                    }
                    Loggers.RAFT.info("get datums from leader: {}, batch size is {}, processedCount is {}, datums' size is {}, RaftCore.datums' size is {}", getLeader().ip, batch.size(), processedCount, beatDatums.size(), datums.size());
                    String url = buildUrl(remote.ip, API_GET); // 调用leader的/raft/datum接口获取最新数据
                    Map<String, String> queryParam = new HashMap<>(1);
                    queryParam.put("keys", URLEncoder.encode(keys, "UTF-8"));
                    HttpClient.asyncHttpGet(url, null, queryParam, new Callback<String>() { // 批量从leader节点获取keys对应的数据更新到本地
                        @Override
                        public void onReceive(RestResult<String> result) {
                            if (!result.ok()) {
                                return;
                            }
                            List<JsonNode> datumList = JacksonUtils.toObj(result.getData(), new TypeReference<List<JsonNode>>() {
                            });
                            for (JsonNode datumJson : datumList) { // 保证集群节点间的数据最终一致性
                                Datum newDatum = null;
                                OPERATE_LOCK.lock();
                                try {
                                    Datum oldDatum = getDatum(datumJson.get("key").asText()); // 获取当前机器上的旧的数据
                                    if (oldDatum != null && datumJson.get("timestamp").asLong() <= oldDatum.timestamp.get()) {
                                        Loggers.RAFT.info("[NACOS-RAFT] timestamp is smaller than that of mine, key: {}, remote: {}, local: {}", datumJson.get("key").asText(), datumJson.get("timestamp").asLong(), oldDatum.timestamp);
                                        continue; // 若旧数据不为null且旧数据的版本大于或等于新数据的版本，则不需要更新直接跳过
                                    }
                                    if (KeyBuilder.matchServiceMetaKey(datumJson.get("key").asText())) { // key以com.alibaba.nacos.naming.domains.meta.或meta.开头
                                        Datum<Service> serviceDatum = new Datum<>();
                                        serviceDatum.key = datumJson.get("key").asText();
                                        serviceDatum.timestamp.set(datumJson.get("timestamp").asLong());
                                        serviceDatum.value = JacksonUtils.toObj(datumJson.get("value").toString(), Service.class);
                                        newDatum = serviceDatum;
                                    }
                                    if (KeyBuilder.matchInstanceListKey(datumJson.get("key").asText())) { // key以com.alibaba.nacos.naming.iplist.或iplist.开头
                                        Datum<Instances> instancesDatum = new Datum<>();
                                        instancesDatum.key = datumJson.get("key").asText();
                                        instancesDatum.timestamp.set(datumJson.get("timestamp").asLong());
                                        instancesDatum.value = JacksonUtils.toObj(datumJson.get("value").toString(), Instances.class);
                                        newDatum = instancesDatum;
                                    }
                                    if (newDatum == null || newDatum.value == null) {
                                        Loggers.RAFT.error("receive null datum: {}", datumJson);
                                        continue; // 跳过空数据
                                    }
                                    raftStore.write(newDatum); // 将数据写入磁盘缓存中
                                    datums.put(newDatum.key, newDatum); // 将新的数据添加到缓存中
                                    notifier.notify(newDatum.key, DataOperation.CHANGE, newDatum.value); // 更新注册表中的数据
                                    local.resetLeaderDue(); // 将leaderDueMs重置为15s + (0到5秒的随机时间) = 15s - 20s
                                    if (local.term.get() + 100 > remote.term.get()) { // 若本地的选举周期加100大于leader节点的选举周期
                                        getLeader().term.set(remote.term.get()); // 将同步leader节点的选举周期
                                        local.term.set(getLeader().term.get()); // 将本地节点的选举周期也置为leader节点的周期
                                    } else {
                                        local.term.addAndGet(100); // 将本地节点的选举周期加100
                                    }
                                    raftStore.updateTerm(local.term.get()); // 更新本地缓存文件meta.properties中的选举周期
                                    Loggers.RAFT.info("data updated, key: {}, timestamp: {}, from {}, local term: {}", newDatum.key, newDatum.timestamp, JacksonUtils.toJson(remote), local.term);
                                } catch (Throwable e) {
                                    Loggers.RAFT.error("[RAFT-BEAT] failed to sync datum from leader, datum: {}", newDatum, e);
                                } finally {
                                    OPERATE_LOCK.unlock();
                                }
                            }
                            try {
                                TimeUnit.MILLISECONDS.sleep(200);
                            } catch (InterruptedException e) {
                                Loggers.RAFT.error("[RAFT-BEAT] Interrupted error ", e);
                            }
                            return;
                        }
                        @Override
                        public void onError(Throwable throwable) {
                            Loggers.RAFT.error("[RAFT-BEAT] failed to sync datum from leader", throwable);
                        }
                        @Override
                        public void onCancel() {
                        }
                    });
                    batch.clear();
                } catch (Exception e) {
                    Loggers.RAFT.error("[NACOS-RAFT] failed to handle beat entry, key: {}", datumKey);
                }
            }
            List<String> deadKeys = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : receivedKeysMap.entrySet()) {
                if (entry.getValue() == 0) { // 未被覆盖的key，说明是已经被删除的数据
                    deadKeys.add(entry.getKey());
                }
            }
            for (String deadKey : deadKeys) {
                try {
                    deleteDatum(deadKey); // 删除数据以及清理key对应的缓存文件
                } catch (Exception e) {
                    Loggers.RAFT.error("[NACOS-RAFT] failed to remove entry, key={} {}", deadKey, e);
                }
            }
        }
        return local;
    }

    /**
     * Add listener for target key.
     *
     * @param key      key
     * @param listener new listener
     */
    public void listen(String key, RecordListener listener) {
        notifier.registerListener(key, listener);

        Loggers.RAFT.info("add listener: {}", key);
        // if data present, notify immediately
        for (Datum datum : datums.values()) {
            if (!listener.interests(datum.key)) {
                continue;
            }

            try {
                listener.onChange(datum.key, datum.value);
            } catch (Exception e) {
                Loggers.RAFT.error("NACOS-RAFT failed to notify listener", e);
            }
        }
    }

    /**
     * Remove listener for key.
     *
     * @param key      key
     * @param listener listener
     */
    public void unListen(String key, RecordListener listener) {
        notifier.deregisterListener(key, listener);
    }

    public void unListenAll(String key) {
        notifier.deregisterAllListener(key);
    }

    public void setTerm(long term) {
        peers.setTerm(term);
    }

    public boolean isLeader(String ip) {
        return peers.isLeader(ip);
    }

    public boolean isLeader() {
        return peers.isLeader(NetUtils.localServer());
    }

    /**
     * Build api url.
     *
     * @param ip  ip of api
     * @param api api path
     * @return api url
     */
    public static String buildUrl(String ip, String api) {
        if (!IPUtil.containsPort(ip)) {
            ip = ip + IPUtil.IP_PORT_SPLITER + EnvUtil.getPort();
        }
        return "http://" + ip + EnvUtil.getContextPath() + api;
    }

    public Datum<?> getDatum(String key) {
        return datums.get(key);
    }

    public RaftPeer getLeader() {
        return peers.getLeader();
    }

    public List<RaftPeer> getPeers() {
        return new ArrayList<>(peers.allPeers());
    }

    public RaftPeerSet getPeerSet() {
        return peers;
    }

    public void setPeerSet(RaftPeerSet peerSet) {
        peers = peerSet;
    }

    public int datumSize() {
        return datums.size();
    }

    public void addDatum(Datum datum) {
        datums.put(datum.key, datum);
        NotifyCenter.publishEvent(ValueChangeEvent.builder().key(datum.key).action(DataOperation.CHANGE).build());
    }

    /**
     * Load datum.
     *
     * @param key datum key
     */
    public void loadDatum(String key) {
        try {
            Datum datum = raftStore.load(key);
            if (datum == null) {
                return;
            }
            datums.put(key, datum);
        } catch (Exception e) {
            Loggers.RAFT.error("load datum failed: " + key, e);
        }

    }

    private void deleteDatum(String key) {
        Datum deleted;
        try {
            deleted = datums.remove(URLDecoder.decode(key, "UTF-8"));
            if (deleted != null) { // 若key对应的数据未被删除
                raftStore.delete(deleted); // 删除缓存数据文件
                Loggers.RAFT.info("datum deleted, key: {}", key);
            }
            NotifyCenter.publishEvent(ValueChangeEvent.builder().key(URLDecoder.decode(key, "UTF-8")).action(DataOperation.DELETE).build());
        } catch (UnsupportedEncodingException e) {
            Loggers.RAFT.warn("datum key decode failed: {}", key);
        }
    }

    public boolean isInitialized() {
        return initialized || !globalConfig.isDataWarmup();
    }

    @Deprecated
    public int getNotifyTaskCount() {
        return (int) publisher.currentEventSize();
    }

}
