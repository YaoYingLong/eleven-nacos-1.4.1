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

package com.alibaba.nacos.naming.consistency.ephemeral.distro;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.utils.Objects;
import com.alibaba.nacos.naming.cluster.ServerStatus;
import com.alibaba.nacos.naming.cluster.transport.Serializer;
import com.alibaba.nacos.consistency.DataOperation;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.consistency.ephemeral.EphemeralConsistencyService;
import com.alibaba.nacos.naming.consistency.ephemeral.distro.combined.DistroHttpCombinedKey;
import com.alibaba.nacos.core.distributed.distro.DistroProtocol;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataProcessor;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.distributed.distro.entity.DistroKey;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.GlobalConfig;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.pojo.Record;
import org.apache.commons.lang3.StringUtils;
import org.javatuples.Pair;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A consistency protocol algorithm called <b>Distro</b>
 *
 * <p>Use a distro algorithm to divide data into many blocks. Each Nacos server node takes responsibility for exactly
 * one block of data. Each block of data is generated, removed and synchronized by its responsible server. So every
 * Nacos server only handles writings for a subset of the total service data.
 *
 * <p>At mean time every Nacos server receives data sync of other Nacos server, so every Nacos server will eventually
 * have a complete set of data.
 *
 * @author nkorange
 * @since 1.0.0
 */
@DependsOn("ProtocolManager")
@org.springframework.stereotype.Service("distroConsistencyService")
public class DistroConsistencyServiceImpl implements EphemeralConsistencyService, DistroDataProcessor {

    private final DistroMapper distroMapper;

    private final DataStore dataStore;

    private final Serializer serializer;

    private final SwitchDomain switchDomain;

    private final GlobalConfig globalConfig;

    private final DistroProtocol distroProtocol;

    private volatile Notifier notifier = new Notifier();

    private Map<String, ConcurrentLinkedQueue<RecordListener>> listeners = new ConcurrentHashMap<>();

    private Map<String, String> syncChecksumTasks = new ConcurrentHashMap<>(16);

    public DistroConsistencyServiceImpl(DistroMapper distroMapper, DataStore dataStore, Serializer serializer,
            SwitchDomain switchDomain, GlobalConfig globalConfig, DistroProtocol distroProtocol) {
        this.distroMapper = distroMapper;
        this.dataStore = dataStore;
        this.serializer = serializer;
        this.switchDomain = switchDomain;
        this.globalConfig = globalConfig;
        this.distroProtocol = distroProtocol;
    }

    @PostConstruct
    public void init() {
        GlobalExecutor.submitDistroNotifyTask(notifier);
    }

    @Override
    public void put(String key, Record value) throws NacosException {
        onPut(key, value); // 若是ephemeral实例将其添加到DataStore缓存中，然后通过一部任务替换注册表中实例列表
        // 同步实例数据到其它服务端成员列表中，是将当前服务名称下所有实例同步到其他服务端，最终是通过异步任务加队列的方式，调用HTTP接口最终在其他服务上也是通过onPut方法来完成
        distroProtocol.sync(new DistroKey(key, KeyBuilder.INSTANCE_LIST_KEY_PREFIX), DataOperation.CHANGE, globalConfig.getTaskDispatchPeriod() / 2);
    }

    @Override
    public void remove(String key) throws NacosException {
        onRemove(key);
        listeners.remove(key);
    }

    @Override
    public Datum get(String key) throws NacosException {
        return dataStore.get(key);
    }

    /**
     * Put a new record.
     *
     * @param key   key of record
     * @param value record
     */
    public void onPut(String key, Record value) {
        if (KeyBuilder.matchEphemeralInstanceListKey(key)) { // 若是ephemeral实例记录
            Datum<Instances> datum = new Datum<>();
            datum.value = (Instances) value;
            datum.key = key;
            datum.timestamp.incrementAndGet();
            dataStore.put(key, datum); // 更新缓存数据
        }
        if (!listeners.containsKey(key)) {
            return;
        }
        notifier.addTask(key, DataOperation.CHANGE);
    }

    /**
     * Remove a record.
     *
     * @param key key of record
     */
    public void onRemove(String key) {
        dataStore.remove(key); // 从缓存中移除
        if (!listeners.containsKey(key)) {
            return;
        }
        notifier.addTask(key, DataOperation.DELETE); // 更注册表
    }

    /**
     * Check sum when receive checksums request.
     *
     * @param checksumMap map of checksum
     * @param server      source server request checksum
     */
    public void onReceiveChecksums(Map<String, String> checksumMap, String server) {
        if (syncChecksumTasks.containsKey(server)) { // Already in process of this server:
            Loggers.DISTRO.warn("sync checksum task already in process with {}", server);
            return;
        }
        syncChecksumTasks.put(server, "1"); // 将任务添加到syncChecksumTasks中
        try {
            List<String> toUpdateKeys = new ArrayList<>(); // 需要更新的列表，包括更新和添加的数据
            List<String> toRemoveKeys = new ArrayList<>(); // 需要移除的列表
            for (Map.Entry<String, String> entry : checksumMap.entrySet()) {
                if (distroMapper.responsible(KeyBuilder.getServiceName(entry.getKey()))) {// this key should not be sent from remote server:
                    Loggers.DISTRO.error("receive responsible key timestamp of " + entry.getKey() + " from " + server);
                    return; // abort the procedure: 当前客户端服务注册在当前的服务端上
                }
                if (!dataStore.contains(entry.getKey()) || dataStore.get(entry.getKey()).value == null || !dataStore.get(entry.getKey()).value.getChecksum().equals(entry.getValue())) {
                    toUpdateKeys.add(entry.getKey()); // 若DataStore缓存中key不存在或key对应的值不存在或值的checksum不相等
                }
            }
            for (String key : dataStore.keys()) {
                if (!server.equals(distroMapper.mapSrv(KeyBuilder.getServiceName(key)))) {
                    continue; // 若当前客户端服务不是注册在发送校验请求的服务端成员上
                }
                if (!checksumMap.containsKey(key)) {
                    toRemoveKeys.add(key); // 若当前客户端服务不在接收列表中，说明已被删除
                }
            }
            Loggers.DISTRO.info("to remove keys: {}, to update keys: {}, source: {}", toRemoveKeys, toUpdateKeys, server);
            for (String key : toRemoveKeys) {
                onRemove(key); // 将需要删除的service数据从缓存中移除
            }
            if (toUpdateKeys.isEmpty()) {
                return; // 若更新列表为空
            }
            try {
                DistroHttpCombinedKey distroKey = new DistroHttpCombinedKey(KeyBuilder.INSTANCE_LIST_KEY_PREFIX, server);
                distroKey.getActualResourceTypes().addAll(toUpdateKeys);
                DistroData remoteData = distroProtocol.queryFromRemote(distroKey); // 调用对应的服务端成员查询数据
                if (null != remoteData) {
                    processData(remoteData.getContent());
                }
            } catch (Exception e) {
                Loggers.DISTRO.error("get data from " + server + " failed!", e);
            }
        } finally {// Remove this 'in process' flag:
            syncChecksumTasks.remove(server);
        }
    }

    private boolean processData(byte[] data) throws Exception {
        if (data.length > 0) {
            Map<String, Datum<Instances>> datumMap = serializer.deserializeMap(data, Instances.class); // 反序列化
            for (Map.Entry<String, Datum<Instances>> entry : datumMap.entrySet()) {
                dataStore.put(entry.getKey(), entry.getValue()); // 替换缓存中的数据
                if (!listeners.containsKey(entry.getKey())) { // 若serviceList中不包含该数据则创建一个并更新到注册表中
                    // pretty sure the service not exist:
                    if (switchDomain.isDefaultInstanceEphemeral()) { // ephemeral为true的实例，默认为true
                        // create empty service
                        Loggers.DISTRO.info("creating service {}", entry.getKey());
                        Service service = new Service();
                        String serviceName = KeyBuilder.getServiceName(entry.getKey());
                        String namespaceId = KeyBuilder.getNamespace(entry.getKey());
                        service.setName(serviceName);
                        service.setNamespaceId(namespaceId);
                        service.setGroupName(Constants.DEFAULT_GROUP);
                        // now validate the service. if failed, exception will be thrown
                        service.setLastModifiedMillis(System.currentTimeMillis());
                        service.recalculateChecksum();
                        // The Listener corresponding to the key value must not be empty
                        RecordListener listener = listeners.get(KeyBuilder.SERVICE_META_KEY_PREFIX).peek();
                        if (Objects.isNull(listener)) {
                            return false;
                        }
                        listener.onChange(KeyBuilder.buildServiceMetaKey(namespaceId, serviceName), service); // 更新注册表数据
                    }
                }
            }
            for (Map.Entry<String, Datum<Instances>> entry : datumMap.entrySet()) { // 处理ephemeral为false的实例
                if (!listeners.containsKey(entry.getKey())) {// Should not happen: 若实例不存在则直接跳过
                    Loggers.DISTRO.warn("listener of {} not found.", entry.getKey());
                    continue;
                }
                try {
                    for (RecordListener listener : listeners.get(entry.getKey())) {
                        listener.onChange(entry.getKey(), entry.getValue().value); // 更新注册表数据
                    }
                } catch (Exception e) {
                    Loggers.DISTRO.error("[NACOS-DISTRO] error while execute listener of key: {}", entry.getKey(), e);
                    continue;
                }
                // Update data store if listener executed successfully:
                dataStore.put(entry.getKey(), entry.getValue()); // 更新缓存中的数据
            }
        }
        return true;
    }

    @Override
    public boolean processData(DistroData distroData) {
        DistroHttpData distroHttpData = (DistroHttpData) distroData;
        Datum<Instances> datum = (Datum<Instances>) distroHttpData.getDeserializedContent();
        onPut(datum.key, datum.value);
        return true;
    }

    @Override
    public String processType() {
        return KeyBuilder.INSTANCE_LIST_KEY_PREFIX;
    }

    @Override
    public boolean processVerifyData(DistroData distroData) {
        DistroHttpData distroHttpData = (DistroHttpData) distroData;
        String sourceServer = distroData.getDistroKey().getResourceKey();
        Map<String, String> verifyData = (Map<String, String>) distroHttpData.getDeserializedContent();
        onReceiveChecksums(verifyData, sourceServer);
        return true;
    }

    @Override
    public boolean processSnapshot(DistroData distroData) {
        try {
            return processData(distroData.getContent());
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void listen(String key, RecordListener listener) throws NacosException {
        if (!listeners.containsKey(key)) {
            listeners.put(key, new ConcurrentLinkedQueue<>());
        }
        if (listeners.get(key).contains(listener)) {
            return;
        }
        listeners.get(key).add(listener);
    }

    @Override
    public void unListen(String key, RecordListener listener) throws NacosException {
        if (!listeners.containsKey(key)) {
            return;
        }
        for (RecordListener recordListener : listeners.get(key)) {
            if (recordListener.equals(listener)) {
                listeners.get(key).remove(listener);
                break;
            }
        }
    }

    @Override
    public boolean isAvailable() {
        return isInitialized() || ServerStatus.UP.name().equals(switchDomain.getOverriddenServerStatus());
    }

    public boolean isInitialized() {
        return distroProtocol.isInitialized() || !globalConfig.isDataWarmup();
    }

    public class Notifier implements Runnable {
        private ConcurrentHashMap<String, String> services = new ConcurrentHashMap<>(10 * 1024);
        private BlockingQueue<Pair<String, DataOperation>> tasks = new ArrayBlockingQueue<>(1024 * 1024);

        /**
         * Add new notify task to queue.
         *
         * @param datumKey data key
         * @param action   action for data
         */
        public void addTask(String datumKey, DataOperation action) {
            if (services.containsKey(datumKey) && action == DataOperation.CHANGE) {
                return; // 若当前Task已存在则直接忽略
            }
            if (action == DataOperation.CHANGE) {
                services.put(datumKey, StringUtils.EMPTY); // 将当前任务添加到services列表中
            }
            tasks.offer(Pair.with(datumKey, action)); // 将当前任务放入阻塞队列
        }

        public int getTaskSize() {
            return tasks.size();
        }

        @Override
        public void run() {
            Loggers.DISTRO.info("distro notifier started");
            for (; ; ) {
                try {
                    Pair<String, DataOperation> pair = tasks.take();
                    handle(pair);
                } catch (Throwable e) {
                    Loggers.DISTRO.error("[NACOS-DISTRO] Error while handling notifying task", e);
                }
            }
        }

        private void handle(Pair<String, DataOperation> pair) {
            try {
                String datumKey = pair.getValue0();
                DataOperation action = pair.getValue1();
                services.remove(datumKey); // 从任务列表移除
                int count = 0;
                if (!listeners.containsKey(datumKey)) {
                    return;
                }
                // 这里的获取到的RecordListener是在创建实例对象时在putServiceAndInit中通过ConsistencyService#listen方法添加的
                for (RecordListener listener : listeners.get(datumKey)) { // listener就是一个Service对象
                    count++;
                    try {
                        if (action == DataOperation.CHANGE) { // 将最新的实例列表数据通过Service的onChange方法进行注册表更新
                            listener.onChange(datumKey, dataStore.get(datumKey).value);
                            continue;
                        }
                        if (action == DataOperation.DELETE) { // 通过Service的onDelete方法进行删除
                            listener.onDelete(datumKey); // 在Service中是空实现
                            continue;
                        }
                    } catch (Throwable e) {
                        Loggers.DISTRO.error("[NACOS-DISTRO] error while notifying listener of key: {}", datumKey, e);
                    }
                }
                if (Loggers.DISTRO.isDebugEnabled()) {
                    Loggers.DISTRO.debug("[NACOS-DISTRO] datum change notified, key: {}, listener count: {}, action: {}", datumKey, count, action.name());
                }
            } catch (Throwable e) {
                Loggers.DISTRO.error("[NACOS-DISTRO] Error while handling notifying task", e);
            }
        }
    }
}
