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
import com.alibaba.nacos.common.http.Callback;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.model.RestResult;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.MemberChangeListener;
import com.alibaba.nacos.core.cluster.MembersChangeEvent;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.naming.misc.HttpClient;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NetUtils;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.alibaba.nacos.sys.utils.ApplicationUtils;
import org.apache.commons.collections.SortedBag;
import org.apache.commons.collections.bag.TreeBag;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sets of raft peers.
 *
 * @author nacos
 * @deprecated will remove in 1.4.x
 */
@Deprecated
@Component
@DependsOn("ProtocolManager")
public class RaftPeerSet extends MemberChangeListener implements Closeable {

    private final ServerMemberManager memberManager;

    private AtomicLong localTerm = new AtomicLong(0L);

    private RaftPeer leader = null;

    private volatile Map<String, RaftPeer> peers = new HashMap<>(8);

    private Set<String> sites = new HashSet<>();

    private volatile boolean ready = false;

    private Set<Member> oldMembers = new HashSet<>();

    public RaftPeerSet(ServerMemberManager memberManager) {
        this.memberManager = memberManager;
    }

    @PostConstruct
    public void init() {
        NotifyCenter.registerSubscriber(this); // 注册当前RaftPeerSet为MemberChangeEvent事件的订阅者
        changePeers(memberManager.allMembers()); // 传入所有服务端成员初始化peers
    }

    @Override
    public void shutdown() throws NacosException {
        this.localTerm.set(-1);
        this.leader = null;
        this.peers.clear();
        this.sites.clear();
        this.ready = false;
        this.oldMembers.clear();
    }

    public RaftPeer getLeader() {
        if (EnvUtil.getStandaloneMode()) {
            return local();
        }
        return leader;
    }

    public Set<String> allSites() {
        return sites;
    }

    public boolean isReady() {
        return ready;
    }

    /**
     * Remove raft node.
     *
     * @param servers node address need to be removed
     */
    public void remove(List<String> servers) {
        for (String server : servers) {
            peers.remove(server);
        }
    }

    /**
     * Update raft peer.
     *
     * @param peer new peer.
     * @return new peer
     */
    public RaftPeer update(RaftPeer peer) {
        peers.put(peer.ip, peer);
        return peer;
    }

    /**
     * Judge whether input address is leader.
     *
     * @param ip peer address
     * @return true if is leader or stand alone, otherwise false
     */
    public boolean isLeader(String ip) {
        if (EnvUtil.getStandaloneMode()) {
            return true;
        }
        if (leader == null) {
            Loggers.RAFT.warn("[IS LEADER] no leader is available now!");
            return false;
        }
        return StringUtils.equals(leader.ip, ip);
    }

    public Set<String> allServersIncludeMyself() {
        return peers.keySet();
    }

    /**
     * Get all servers excludes current peer.
     *
     * @return all servers excludes current peer
     */
    public Set<String> allServersWithoutMySelf() {
        Set<String> servers = new HashSet<String>(peers.keySet());
        // exclude myself
        servers.remove(local().ip);
        return servers;
    }

    public Collection<RaftPeer> allPeers() {
        return peers.values();
    }

    public int size() {
        return peers.size();
    }

    /**
     * Calculate and decide which peer is leader. If has new peer has more than half vote, change leader to new peer.
     *
     * @param candidate new candidate
     * @return new leader if new candidate has more than half vote, otherwise old leader
     */
    public RaftPeer decideLeader(RaftPeer candidate) {
        peers.put(candidate.ip, candidate);
        SortedBag ips = new TreeBag();
        int maxApproveCount = 0;
        String maxApprovePeer = null;
        for (RaftPeer peer : peers.values()) { // 遍历所有节点，若voteFor不为null，则将节点的voteFor添加到ips中，并记录被选举次数最多的节点和次数
            if (StringUtils.isEmpty(peer.voteFor)) {
                continue; // 若投票结果为null则直接跳过
            }
            ips.add(peer.voteFor); // 将投票添加到ips中
            if (ips.getCount(peer.voteFor) > maxApproveCount) {
                maxApproveCount = ips.getCount(peer.voteFor);
                maxApprovePeer = peer.voteFor;
            }
        }
        if (maxApproveCount >= majorityCount()) { // 若票数超过一半
            RaftPeer peer = peers.get(maxApprovePeer); // 获取出该节点
            peer.state = RaftPeer.State.LEADER;  // 将该节点状态设置为leader节点
            if (!Objects.equals(leader, peer)) {
                leader = peer; // 若leader节点不是改节点，将leader节点设置为该节点
                ApplicationUtils.publishEvent(new LeaderElectFinishedEvent(this, leader, local())); // 发布选举结果事件，RaftListener
                Loggers.RAFT.info("{} has become the LEADER", leader.ip);
            }
        }
        return leader;
    }

    /**
     * Set leader as new candidate.
     *
     * @param candidate new candidate
     * @return new leader
     */
    public RaftPeer makeLeader(RaftPeer candidate) {
        if (!Objects.equals(leader, candidate)) {
            leader = candidate;
            ApplicationUtils.publishEvent(new MakeLeaderEvent(this, leader, local()));
            Loggers.RAFT.info("{} has become the LEADER, local: {}, leader: {}", leader.ip, JacksonUtils.toJson(local()), JacksonUtils.toJson(leader));
        }
        for (final RaftPeer peer : peers.values()) {
            Map<String, String> params = new HashMap<>(1);
            if (!Objects.equals(peer, candidate) && peer.state == RaftPeer.State.LEADER) {
                try {
                    String url = RaftCore.buildUrl(peer.ip, RaftCore.API_GET_PEER);
                    HttpClient.asyncHttpGet(url, null, params, new Callback<String>() {
                        @Override
                        public void onReceive(RestResult<String> result) {
                            if (!result.ok()) {
                                Loggers.RAFT.error("[NACOS-RAFT] get peer failed: {}, peer: {}", result.getCode(), peer.ip);
                                peer.state = RaftPeer.State.FOLLOWER;
                                return;
                            }
                            update(JacksonUtils.toObj(result.getData(), RaftPeer.class));
                        }

                        @Override
                        public void onError(Throwable throwable) {
                        }

                        @Override
                        public void onCancel() {
                        }
                    });
                } catch (Exception e) {
                    peer.state = RaftPeer.State.FOLLOWER;
                    Loggers.RAFT.error("[NACOS-RAFT] error while getting peer from peer: {}", peer.ip);
                }
            }
        }
        return update(candidate);
    }

    /**
     * Get local raft peer.
     *
     * @return local raft peer
     */
    public RaftPeer local() { // peers在init()方法中已被初始化
        RaftPeer peer = peers.get(EnvUtil.getLocalAddress()); // 通过本机地址获取RaftPeer
        if (peer == null && EnvUtil.getStandaloneMode()) { // 若通过本机地址未获取到RaftPeer且是Standalone模式启动
            RaftPeer localPeer = new RaftPeer(); // 新建一个RaftPeer将其ip设置为本机地址
            localPeer.ip = NetUtils.localServer();
            localPeer.term.set(localTerm.get()); // 开始周期term为0
            peers.put(localPeer.ip, localPeer);
            return localPeer;
        }
        if (peer == null) {
            throw new IllegalStateException("unable to find local peer: " + NetUtils.localServer() + ", all peers: " + Arrays.toString(peers.keySet().toArray()));
        }
        return peer;
    }

    public RaftPeer get(String server) {
        return peers.get(server);
    }

    public int majorityCount() {
        return peers.size() / 2 + 1;
    }

    /**
     * Reset set.
     */
    public void reset() { // 将leader和所有的RaftPeer的voteFor字段置null
        leader = null;
        for (RaftPeer peer : peers.values()) {
            peer.voteFor = null;
        }
    }

    public void setTerm(long term) {
        localTerm.set(term);
    }

    public long getTerm() {
        return localTerm.get();
    }

    public boolean contains(RaftPeer remote) {
        return peers.containsKey(remote.ip);
    }

    @Override
    public void onEvent(MembersChangeEvent event) {
        Collection<Member> members = event.getMembers();
        Collection<Member> newMembers = new HashSet<>(members);
        newMembers.removeAll(oldMembers);
        // If an IP change occurs, the change starts
        if (!newMembers.isEmpty()) {
            changePeers(members);
        }
        oldMembers.clear();
        oldMembers.addAll(members);
    }

    protected void changePeers(Collection<Member> members) {
        Map<String, RaftPeer> tmpPeers = new HashMap<>(members.size());
        for (Member member : members) {
            final String address = member.getAddress();
            if (peers.containsKey(address)) { // 若已包含则获取原来的设置到tmpPeers中
                tmpPeers.put(address, peers.get(address));
                continue;
            }
            RaftPeer raftPeer = new RaftPeer(); // 若不存在则创建一个RaftPeer
            raftPeer.ip = address; // 初始化ip为当前成员的地址
            // first time meet the local server:
            if (EnvUtil.getLocalAddress().equals(address)) {
                raftPeer.term.set(localTerm.get()); // 若当前成员为本机则设置周期term，localTerm起始为0
            }
            tmpPeers.put(address, raftPeer);
        }
        // replace raft peer set:
        peers = tmpPeers;
        ready = true;
        Loggers.RAFT.info("raft peers changed: " + members);
    }

    @Override
    public String toString() {
        return "RaftPeerSet{" + "localTerm=" + localTerm + ", leader=" + leader + ", peers=" + peers + ", sites=" + sites + '}';
    }
}
