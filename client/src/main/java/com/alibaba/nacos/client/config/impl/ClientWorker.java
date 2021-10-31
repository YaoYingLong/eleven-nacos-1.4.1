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

package com.alibaba.nacos.client.config.impl;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.config.ConfigType;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.client.config.common.GroupKey;
import com.alibaba.nacos.client.config.filter.impl.ConfigFilterChainManager;
import com.alibaba.nacos.client.config.http.HttpAgent;
import com.alibaba.nacos.client.config.utils.ContentUtils;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.client.utils.ParamUtil;
import com.alibaba.nacos.client.utils.TenantUtil;
import com.alibaba.nacos.common.http.HttpRestResult;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.common.utils.MD5Utils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.api.common.Constants.CONFIG_TYPE;
import static com.alibaba.nacos.api.common.Constants.LINE_SEPARATOR;
import static com.alibaba.nacos.api.common.Constants.WORD_SEPARATOR;

/**
 * Long polling.
 *
 * @author Nacos
 */
public class ClientWorker implements Closeable {

    private static final Logger LOGGER = LogUtils.logger(ClientWorker.class);

    /**
     * Add listeners for data.
     *
     * @param dataId    dataId of data
     * @param group     group of data
     * @param listeners listeners
     */
    public void addListeners(String dataId, String group, List<? extends Listener> listeners) {
        group = null2defaultGroup(group);
        CacheData cache = addCacheDataIfAbsent(dataId, group);
        for (Listener listener : listeners) {
            cache.addListener(listener);
        }
    }

    /**
     * Remove listener.
     *
     * @param dataId   dataId of data
     * @param group    group of data
     * @param listener listener
     */
    public void removeListener(String dataId, String group, Listener listener) {
        group = null2defaultGroup(group);
        CacheData cache = getCache(dataId, group);
        if (null != cache) {
            cache.removeListener(listener);
            if (cache.getListeners().isEmpty()) {
                removeCache(dataId, group);
            }
        }
    }

    /**
     * Add listeners for tenant.
     *
     * @param dataId    dataId of data
     * @param group     group of data
     * @param listeners listeners
     * @throws NacosException nacos exception
     */
    public void addTenantListeners(String dataId, String group, List<? extends Listener> listeners) throws NacosException {
        // 被NacosConfigService的addListener方法调用，
        group = null2defaultGroup(group);
        String tenant = agent.getTenant();
        CacheData cache = addCacheDataIfAbsent(dataId, group, tenant);
        // 这里的Listener是在NacosContextRefresher中onApplicationEvent中监听ApplicationReadyEvent事件时为每一个dataId注册了一个AbstractSharedListener监听器
        for (Listener listener : listeners) {
            cache.addListener(listener);
        }
    }

    /**
     * Add listeners for tenant with content.
     *
     * @param dataId    dataId of data
     * @param group     group of data
     * @param content   content
     * @param listeners listeners
     * @throws NacosException nacos exception
     */
    public void addTenantListenersWithContent(String dataId, String group, String content,
            List<? extends Listener> listeners) throws NacosException {
        group = null2defaultGroup(group);
        String tenant = agent.getTenant();
        CacheData cache = addCacheDataIfAbsent(dataId, group, tenant);
        cache.setContent(content);
        for (Listener listener : listeners) {
            cache.addListener(listener);
        }
    }

    /**
     * Remove listeners for tenant.
     *
     * @param dataId   dataId of data
     * @param group    group of data
     * @param listener listener
     */
    public void removeTenantListener(String dataId, String group, Listener listener) {
        group = null2defaultGroup(group);
        String tenant = agent.getTenant();
        CacheData cache = getCache(dataId, group, tenant);
        if (null != cache) {
            cache.removeListener(listener);
            if (cache.getListeners().isEmpty()) {
                removeCache(dataId, group, tenant);
            }
        }
    }

    private void removeCache(String dataId, String group) {
        String groupKey = GroupKey.getKey(dataId, group);
        cacheMap.remove(groupKey);
        LOGGER.info("[{}] [unsubscribe] {}", this.agent.getName(), groupKey);
        MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.size());
    }

    void removeCache(String dataId, String group, String tenant) {
        String groupKey = GroupKey.getKeyTenant(dataId, group, tenant);
        cacheMap.remove(groupKey);
        LOGGER.info("[{}] [unsubscribe] {}", agent.getName(), groupKey);

        MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.size());
    }

    /**
     * Add cache data if absent.
     *
     * @param dataId data id if data
     * @param group  group of data
     * @return cache data
     */
    public CacheData addCacheDataIfAbsent(String dataId, String group) {
        String key = GroupKey.getKey(dataId, group);
        CacheData cacheData = cacheMap.get(key);
        if (cacheData != null) {
            return cacheData;
        }

        cacheData = new CacheData(configFilterChainManager, agent.getName(), dataId, group);
        // multiple listeners on the same dataid+group and race condition
        CacheData lastCacheData = cacheMap.putIfAbsent(key, cacheData);
        if (lastCacheData == null) {
            int taskId = cacheMap.size() / (int) ParamUtil.getPerTaskConfigSize();
            lastCacheData = cacheData;
            lastCacheData.setTaskId(taskId);
        }
        // reset so that server not hang this check
        lastCacheData.setInitializing(true);

        LOGGER.info("[{}] [subscribe] {}", this.agent.getName(), key);

        MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.size());
        return lastCacheData;
    }

    /**
     * Add cache data if absent.
     *
     * @param dataId data id if data
     * @param group  group of data
     * @param tenant tenant of data
     * @return cache data
     */
    public CacheData addCacheDataIfAbsent(String dataId, String group, String tenant) throws NacosException {
        String key = GroupKey.getKeyTenant(dataId, group, tenant);
        CacheData cacheData = cacheMap.get(key);
        if (cacheData != null) {
            return cacheData;
        }

        cacheData = new CacheData(configFilterChainManager, agent.getName(), dataId, group, tenant);
        // multiple listeners on the same dataid+group and race condition
        CacheData lastCacheData = cacheMap.putIfAbsent(key, cacheData);
        if (lastCacheData == null) {
            //fix issue # 1317
            if (enableRemoteSyncConfig) {
                String[] ct = getServerConfig(dataId, group, tenant, 3000L);
                cacheData.setContent(ct[0]);
            }
            int taskId = cacheMap.size() / (int) ParamUtil.getPerTaskConfigSize();
            cacheData.setTaskId(taskId);
            lastCacheData = cacheData;
        }

        // reset so that server not hang this check
        lastCacheData.setInitializing(true);

        LOGGER.info("[{}] [subscribe] {}", agent.getName(), key);
        MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.size());

        return lastCacheData;
    }

    public CacheData getCache(String dataId, String group) {
        return getCache(dataId, group, TenantUtil.getUserTenantForAcm());
    }

    public CacheData getCache(String dataId, String group, String tenant) {
        if (null == dataId || null == group) {
            throw new IllegalArgumentException();
        }
        return cacheMap.get(GroupKey.getKeyTenant(dataId, group, tenant));
    }

    public String[] getServerConfig(String dataId, String group, String tenant, long readTimeout) throws NacosException {
        String[] ct = new String[2];
        if (StringUtils.isBlank(group)) {
            group = Constants.DEFAULT_GROUP; // 若group为空则设置为默认值DEFAULT_GROUP
        }
        HttpRestResult<String> result = null;
        try {
            Map<String, String> params = new HashMap<String, String>(3);
            if (StringUtils.isBlank(tenant)) {
                params.put("dataId", dataId);
                params.put("group", group);
            } else {
                params.put("dataId", dataId);
                params.put("group", group);
                params.put("tenant", tenant);
            }
            // 调用服务端/v1/cs/configs接口获取最新的配置信息
            result = agent.httpGet(Constants.CONFIG_CONTROLLER_PATH, null, params, agent.getEncode(), readTimeout);
        } catch (Exception ex) {
            String message = String.format("[%s] [sub-server] get server config exception, dataId=%s, group=%s, tenant=%s", agent.getName(), dataId, group, tenant);
            LOGGER.error(message, ex);
            throw new NacosException(NacosException.SERVER_ERROR, ex);
        }
        switch (result.getCode()) {
            case HttpURLConnection.HTTP_OK: // 保存结果到本地文件中
                LocalConfigInfoProcessor.saveSnapshot(agent.getName(), dataId, group, tenant, result.getData());
                ct[0] = result.getData();
                if (result.getHeader().getValue(CONFIG_TYPE) != null) {
                    ct[1] = result.getHeader().getValue(CONFIG_TYPE);
                } else {
                    ct[1] = ConfigType.TEXT.getType();
                }
                return ct;
            case HttpURLConnection.HTTP_NOT_FOUND:
                LocalConfigInfoProcessor.saveSnapshot(agent.getName(), dataId, group, tenant, null);
                return ct;
            case HttpURLConnection.HTTP_CONFLICT: {
                LOGGER.error("[{}] [sub-server-error] get server config being modified concurrently, dataId={}, group={}, tenant={}", agent.getName(), dataId, group, tenant);
                throw new NacosException(NacosException.CONFLICT, "data being modified, dataId=" + dataId + ",group=" + group + ",tenant=" + tenant);
            }
            case HttpURLConnection.HTTP_FORBIDDEN: {
                LOGGER.error("[{}] [sub-server-error] no right, dataId={}, group={}, tenant={}", agent.getName(), dataId, group, tenant);
                throw new NacosException(result.getCode(), result.getMessage());
            }
            default: {
                LOGGER.error("[{}] [sub-server-error]  dataId={}, group={}, tenant={}, code={}", agent.getName(), dataId, group, tenant, result.getCode());
                throw new NacosException(result.getCode(), "http error, code=" + result.getCode() + ",dataId=" + dataId + ",group=" + group + ",tenant=" + tenant);
            }
        }
    }

    private void checkLocalConfig(CacheData cacheData) {
        final String dataId = cacheData.dataId;
        final String group = cacheData.group;
        final String tenant = cacheData.tenant;
        File path = LocalConfigInfoProcessor.getFailoverFile(agent.getName(), dataId, group, tenant); // 获取本地缓存的配置文件
        if (!cacheData.isUseLocalConfigInfo() && path.exists()) {  // isUseLocalConfig默认为false
            String content = LocalConfigInfoProcessor.getFailover(agent.getName(), dataId, group, tenant); // 读取本地缓存的配置文件内容
            final String md5 = MD5Utils.md5Hex(content, Constants.ENCODE);
            cacheData.setUseLocalConfigInfo(true);
            cacheData.setLocalConfigInfoVersion(path.lastModified());
            cacheData.setContent(content);
            LOGGER.warn("[{}] [failover-change] failover file created. dataId={}, group={}, tenant={}, md5={}, content={}", agent.getName(), dataId, group, tenant, md5, ContentUtils.truncateContent(content));
            return;
        }
        // If use local config info, then it doesn't notify business listener and notify after getting from server.
        if (cacheData.isUseLocalConfigInfo() && !path.exists()) {
            cacheData.setUseLocalConfigInfo(false);
            LOGGER.warn("[{}] [failover-change] failover file deleted. dataId={}, group={}, tenant={}", agent.getName(), dataId, group, tenant);
            return;
        }
        // When it changed. 当本地缓存的配置文件最后修改时间和缓存中的时间不一致，说明文件被更新了，重新从缓存文件中加载数据到缓存中
        if (cacheData.isUseLocalConfigInfo() && path.exists() && cacheData.getLocalConfigInfoVersion() != path.lastModified()) {
            String content = LocalConfigInfoProcessor.getFailover(agent.getName(), dataId, group, tenant);
            final String md5 = MD5Utils.md5Hex(content, Constants.ENCODE);
            cacheData.setUseLocalConfigInfo(true);
            cacheData.setLocalConfigInfoVersion(path.lastModified());
            cacheData.setContent(content);
            LOGGER.warn("[{}] [failover-change] failover file changed. dataId={}, group={}, tenant={}, md5={}, content={}", agent.getName(), dataId, group, tenant, md5, ContentUtils.truncateContent(content));
        }
    }

    private String null2defaultGroup(String group) {
        return (null == group) ? Constants.DEFAULT_GROUP : group.trim();
    }

    /**
     * Check config info.
     */
    public void checkConfigInfo() {
        // Dispatch taskes. cacheMap中缓存着需要刷新的配置，将cacheMap中的数量以3000分一个组，分别创建一个LongPollingRunnable用来监听配置更新
        int listenerSize = cacheMap.size();
        // Round up the longingTaskCount.
        int longingTaskCount = (int) Math.ceil(listenerSize / ParamUtil.getPerTaskConfigSize()); // perTaskConfigSize默认为3000
        if (longingTaskCount > currentLongingTaskCount) {
            for (int i = (int) currentLongingTaskCount; i < longingTaskCount; i++) {
                // The task list is no order.So it maybe has issues when changing.
                executorService.execute(new LongPollingRunnable(i));
            }
            currentLongingTaskCount = longingTaskCount;
        }
    }

    /**
     * Fetch the dataId list from server.
     *
     * @param cacheDatas              CacheDatas for config infomations.
     * @param inInitializingCacheList initial cache lists.
     * @return String include dataId and group (ps: it maybe null).
     * @throws Exception Exception.
     */
    List<String> checkUpdateDataIds(List<CacheData> cacheDatas, List<String> inInitializingCacheList) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (CacheData cacheData : cacheDatas) {
            if (!cacheData.isUseLocalConfigInfo()) {
                sb.append(cacheData.dataId).append(WORD_SEPARATOR);
                sb.append(cacheData.group).append(WORD_SEPARATOR);
                if (StringUtils.isBlank(cacheData.tenant)) { // 若不存在namespace
                    sb.append(cacheData.getMd5()).append(LINE_SEPARATOR);
                } else {
                    sb.append(cacheData.getMd5()).append(WORD_SEPARATOR);
                    sb.append(cacheData.getTenant()).append(LINE_SEPARATOR);
                }
                if (cacheData.isInitializing()) { // isInitializing默认为true
                    // It updates when cacheData occours in cacheMap by first time.
                    inInitializingCacheList.add(GroupKey.getKeyTenant(cacheData.dataId, cacheData.group, cacheData.tenant));
                }
            }
        }
        boolean isInitializingCacheList = !inInitializingCacheList.isEmpty();
        return checkUpdateConfigStr(sb.toString(), isInitializingCacheList);
    }

    /**
     * Fetch the updated dataId list from server.
     *
     * @param probeUpdateString       updated attribute string value.
     * @param isInitializingCacheList initial cache lists.
     * @return The updated dataId list(ps: it maybe null).
     * @throws IOException Exception.
     */
    List<String> checkUpdateConfigStr(String probeUpdateString, boolean isInitializingCacheList) throws Exception {
        Map<String, String> params = new HashMap<String, String>(2);
        params.put(Constants.PROBE_MODIFY_REQUEST, probeUpdateString); // Listening-Configs
        Map<String, String> headers = new HashMap<String, String>(2);
        headers.put("Long-Pulling-Timeout", "" + timeout); // timeout默认为30000ms即30s
        // told server do not hang me up if new initializing cacheData added in
        if (isInitializingCacheList) {
            headers.put("Long-Pulling-Timeout-No-Hangup", "true");
        }
        if (StringUtils.isBlank(probeUpdateString)) {
            return Collections.emptyList(); // 若没有数据则直接返回
        }
        try {
            // In order to prevent the server from handling the delay of the client's long task,
            // increase the client's read timeout to avoid this problem.
            long readTimeoutMs = timeout + (long) Math.round(timeout >> 1); // 默认为45s
            // 调用注册中心的/v1/cs/configs/listener接口
            HttpRestResult<String> result = agent.httpPost(Constants.CONFIG_CONTROLLER_PATH + "/listener", headers, params, agent.getEncode(), readTimeoutMs);
            if (result.ok()) {
                setHealthServer(true);
                return parseUpdateDataIdResponse(result.getData()); // 解析服务端返回的数据
            } else {
                setHealthServer(false);
                LOGGER.error("[{}] [check-update] get changed dataId error, code: {}", agent.getName(), result.getCode());
            }
        } catch (Exception e) {
            setHealthServer(false);
            LOGGER.error("[" + agent.getName() + "] [check-update] get changed dataId exception", e);
            throw e;
        }
        return Collections.emptyList();
    }

    /**
     * Get the groupKey list from the http response.
     *
     * @param response Http response.
     * @return GroupKey List, (ps: it maybe null).
     */
    private List<String> parseUpdateDataIdResponse(String response) {
        if (StringUtils.isBlank(response)) {
            return Collections.emptyList();
        }
        try {
            response = URLDecoder.decode(response, "UTF-8");
        } catch (Exception e) {
            LOGGER.error("[" + agent.getName() + "] [polling-resp] decode modifiedDataIdsString error", e);
        }
        List<String> updateList = new LinkedList<String>();
        for (String dataIdAndGroup : response.split(LINE_SEPARATOR)) {
            if (!StringUtils.isBlank(dataIdAndGroup)) {
                String[] keyArr = dataIdAndGroup.split(WORD_SEPARATOR);
                String dataId = keyArr[0];
                String group = keyArr[1];
                if (keyArr.length == 2) {
                    updateList.add(GroupKey.getKey(dataId, group));
                    LOGGER.info("[{}] [polling-resp] config changed. dataId={}, group={}", agent.getName(), dataId, group);
                } else if (keyArr.length == 3) {
                    String tenant = keyArr[2];
                    updateList.add(GroupKey.getKeyTenant(dataId, group, tenant));
                    LOGGER.info("[{}] [polling-resp] config changed. dataId={}, group={}, tenant={}", agent.getName(), dataId, group, tenant);
                } else {
                    LOGGER.error("[{}] [polling-resp] invalid dataIdAndGroup error {}", agent.getName(), dataIdAndGroup);
                }
            }
        }
        return updateList;
    }

    @SuppressWarnings("PMD.ThreadPoolCreationRule")
    public ClientWorker(final HttpAgent agent, final ConfigFilterChainManager configFilterChainManager, final Properties properties) {
        this.agent = agent;
        this.configFilterChainManager = configFilterChainManager;
        init(properties); // Initialize the timeout parameter
        this.executor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("com.alibaba.nacos.client.Worker." + agent.getName());
                t.setDaemon(true);
                return t;
            }
        });
        // 线程数等于处理器个数的线程池，用来执行LongPollingRunnable#run方法，在checkConfigInfo方法中被放入线程中
        this.executorService = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors(), new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r);
                        t.setName("com.alibaba.nacos.client.Worker.longPolling." + agent.getName());
                        t.setDaemon(true);
                        return t;
                    }
                });
        this.executor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    checkConfigInfo(); // 每10ms执行一次定时任务，将cacheMap中的数量以3000分一个组，分别创建一个LongPollingRunnable用来监听配置更新
                } catch (Throwable e) {
                    LOGGER.error("[" + agent.getName() + "] [sub-check] rotate check error", e);
                }
            }
        }, 1L, 10L, TimeUnit.MILLISECONDS);
    }

    private void init(Properties properties) {
        // timeout默认为30000，最小为10000
        timeout = Math.max(ConvertUtils.toInt(properties.getProperty(PropertyKeyConst.CONFIG_LONG_POLL_TIMEOUT), Constants.CONFIG_LONG_POLL_TIMEOUT), Constants.MIN_CONFIG_LONG_POLL_TIMEOUT);
        // 默认2000ms
        taskPenaltyTime = ConvertUtils.toInt(properties.getProperty(PropertyKeyConst.CONFIG_RETRY_TIME), Constants.CONFIG_RETRY_TIME);
        this.enableRemoteSyncConfig = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.ENABLE_REMOTE_SYNC_CONFIG));
    }

    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        LOGGER.info("{} do shutdown begin", className);
        ThreadUtils.shutdownThreadPool(executorService, LOGGER);
        ThreadUtils.shutdownThreadPool(executor, LOGGER);
        LOGGER.info("{} do shutdown stop", className);
    }

    /**
     * 长轮训主要有4个步骤：
     * 1.检查本地配置，若存在本地配置，且与缓存中的本地配置版本不一致，把本地配置内容更新到缓存中，并触发事件
     * 2.向Nacos Server发送长连接，30s超时，Nacos Server会返回变化的dataIds
     * 3.根据变化的dataId，从服务端拉取最新的配置内容，并更新本地快照和缓存
     * 4.对有变化的配置处罚监听事件类处理
     */
    class LongPollingRunnable implements Runnable {
        private final int taskId;
        public LongPollingRunnable(int taskId) {
            this.taskId = taskId;
        }
        @Override
        public void run() { // 客户端pull长轮询，出现异常延迟2s执行
            List<CacheData> cacheDatas = new ArrayList<CacheData>();
            List<String> inInitializingCacheList = new ArrayList<String>();
            try {
                // check failover config
                for (CacheData cacheData : cacheMap.values()) {
                    if (cacheData.getTaskId() == taskId) {
                        cacheDatas.add(cacheData);
                        try {
                            checkLocalConfig(cacheData); // 容错配置，用于检测本地的配置，若本地配置更新则更新缓存
                            if (cacheData.isUseLocalConfigInfo()) {
                                cacheData.checkListenerMd5(); // 对于变化的配置调用对应的监听器去处理
                            }
                        } catch (Exception e) {
                            LOGGER.error("get local config info error", e);
                        }
                    }
                }
                // check server config，向Nacos Server发一个长连接30s超时，返回Nacos Server中有更新过的dataIds
                List<String> changedGroupKeys = checkUpdateDataIds(cacheDatas, inInitializingCacheList);
                if (!CollectionUtils.isEmpty(changedGroupKeys)) {
                    LOGGER.info("get changedGroupKeys:" + changedGroupKeys);
                }
                for (String groupKey : changedGroupKeys) { // 遍历变更列表
                    String[] key = GroupKey.parseKey(groupKey);
                    String dataId = key[0];
                    String group = key[1];
                    String tenant = null;
                    if (key.length == 3) {
                        tenant = key[2];
                    }
                    try { // 根据变化的dataId调用Nacos Config服务端获取配置信息，并更新本地快照
                        String[] ct = getServerConfig(dataId, group, tenant, 3000L);
                        CacheData cache = cacheMap.get(GroupKey.getKeyTenant(dataId, group, tenant));
                        cache.setContent(ct[0]); // 更新缓存数据
                        if (null != ct[1]) {
                            cache.setType(ct[1]);
                        }
                        LOGGER.info("[{}] [data-received] dataId={}, group={}, tenant={}, md5={}, content={}, type={}", agent.getName(), dataId, group, tenant, cache.getMd5(), ContentUtils.truncateContent(ct[0]), ct[1]);
                    } catch (NacosException ioe) {
                        String message = String.format("[%s] [get-update] get changed config exception. dataId=%s, group=%s, tenant=%s", agent.getName(), dataId, group, tenant);
                        LOGGER.error(message, ioe);
                    }
                }
                for (CacheData cacheData : cacheDatas) {
                    if (!cacheData.isInitializing() || inInitializingCacheList.contains(GroupKey.getKeyTenant(cacheData.dataId, cacheData.group, cacheData.tenant))) {
                        cacheData.checkListenerMd5(); // 对于变化的配置调用对应的监听器去处理
                        cacheData.setInitializing(false);
                    }
                }
                inInitializingCacheList.clear();
                executorService.execute(this);  // 若正常则延迟10ms执行
            } catch (Throwable e) {
                // If the rotation training task is abnormal, the next execution time of the task will be punished
                LOGGER.error("longPolling error : ", e);
                executorService.schedule(this, taskPenaltyTime, TimeUnit.MILLISECONDS); // 若异常则延迟2s执行
            }
        }
    }

    public boolean isHealthServer() {
        return isHealthServer;
    }

    private void setHealthServer(boolean isHealthServer) {
        this.isHealthServer = isHealthServer;
    }

    final ScheduledExecutorService executor;

    final ScheduledExecutorService executorService;

    /**
     * groupKey -> cacheData.
     */
    private final ConcurrentHashMap<String, CacheData> cacheMap = new ConcurrentHashMap<String, CacheData>();

    private final HttpAgent agent;

    private final ConfigFilterChainManager configFilterChainManager;

    private boolean isHealthServer = true;

    private long timeout;

    private double currentLongingTaskCount = 0;

    private int taskPenaltyTime;

    private boolean enableRemoteSyncConfig = false;
}
