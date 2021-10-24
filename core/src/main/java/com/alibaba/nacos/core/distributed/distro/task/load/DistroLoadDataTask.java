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

package com.alibaba.nacos.core.distributed.distro.task.load;

import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.distributed.distro.DistroConfig;
import com.alibaba.nacos.core.distributed.distro.component.DistroCallback;
import com.alibaba.nacos.core.distributed.distro.component.DistroComponentHolder;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataProcessor;
import com.alibaba.nacos.core.distributed.distro.component.DistroTransportAgent;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.utils.GlobalExecutor;
import com.alibaba.nacos.core.utils.Loggers;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Distro load data task.
 *
 * @author xiweng.yy
 */
public class DistroLoadDataTask implements Runnable {

    private final ServerMemberManager memberManager;

    private final DistroComponentHolder distroComponentHolder;

    private final DistroConfig distroConfig;

    private final DistroCallback loadCallback;

    private final Map<String, Boolean> loadCompletedMap;

    public DistroLoadDataTask(ServerMemberManager memberManager, DistroComponentHolder distroComponentHolder, DistroConfig distroConfig, DistroCallback loadCallback) {
        this.memberManager = memberManager;
        this.distroComponentHolder = distroComponentHolder;
        this.distroConfig = distroConfig;
        this.loadCallback = loadCallback;
        loadCompletedMap = new HashMap<>(1);
    }

    @Override
    public void run() {
        try {
            load(); // 加载数据
            if (!checkCompleted()) { // 若数据加载失败则重试
                GlobalExecutor.submitLoadDataTask(this, distroConfig.getLoadDataRetryDelayMillis());
            } else {
                loadCallback.onSuccess(); // 加载成功将isInitialized设置为true
                Loggers.DISTRO.info("[DISTRO-INIT] load snapshot data success");
            }
        } catch (Exception e) {
            loadCallback.onFailed(e); // 异常将isInitialized设置为false
            Loggers.DISTRO.error("[DISTRO-INIT] load snapshot data failed. ", e);
        }
    }

    private void load() throws Exception {
        while (memberManager.allMembersWithoutSelf().isEmpty()) { // 等待成员数据加载完成
            Loggers.DISTRO.info("[DISTRO-INIT] waiting server list init...");
            TimeUnit.SECONDS.sleep(1);
        }
        while (distroComponentHolder.getDataStorageTypes().isEmpty()) { // 等待缓存实例加载完成
            Loggers.DISTRO.info("[DISTRO-INIT] waiting distro data storage register...");
            TimeUnit.SECONDS.sleep(1);
        }
        for (String each : distroComponentHolder.getDataStorageTypes()) {
            if (!loadCompletedMap.containsKey(each) || !loadCompletedMap.get(each)) {
                loadCompletedMap.put(each, loadAllDataSnapshotFromRemote(each)); // 从其他服务端成员加载数据镜像
            }
        }
    }

    private boolean loadAllDataSnapshotFromRemote(String resourceType) {
        DistroTransportAgent transportAgent = distroComponentHolder.findTransportAgent(resourceType);
        DistroDataProcessor dataProcessor = distroComponentHolder.findDataProcessor(resourceType);
        if (null == transportAgent || null == dataProcessor) {
            Loggers.DISTRO.warn("[DISTRO-INIT] Can't find component for type {}, transportAgent: {}, dataProcessor: {}", resourceType, transportAgent, dataProcessor);
            return false;
        }
        for (Member each : memberManager.allMembersWithoutSelf()) { // 遍历除自己以外的所有其它服务端成员
            try {
                Loggers.DISTRO.info("[DISTRO-INIT] load snapshot {} from {}", resourceType, each.getAddress());
                DistroData distroData = transportAgent.getDatumSnapshot(each.getAddress()); // 通过HTTP接口获取目标成员中的注册服务数据
                boolean result = dataProcessor.processSnapshot(distroData); // 解析数据并更新到缓存和注册表中
                Loggers.DISTRO.info("[DISTRO-INIT] load snapshot {} from {} result: {}", resourceType, each.getAddress(), result);
                if (result) {
                    return true; // 只要有一个成功则退出循环
                }
            } catch (Exception e) {
                Loggers.DISTRO.error("[DISTRO-INIT] load snapshot {} from {} failed.", resourceType, each.getAddress(), e);
            }
        }
        return false;
    }

    private boolean checkCompleted() {
        if (distroComponentHolder.getDataStorageTypes().size() != loadCompletedMap.size()) {
            return false;
        }
        for (Boolean each : loadCompletedMap.values()) {
            if (!each) {
                return false;
            }
        }
        return true;
    }
}
