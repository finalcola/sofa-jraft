/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.rhea.client.pd;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rhea.errors.RouteTableException;
import com.alipay.sofa.jraft.rhea.metadata.Cluster;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.Store;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions;
import com.alipay.sofa.jraft.rhea.options.RegionEngineOptions;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.Strings;
import com.alipay.sofa.jraft.util.Endpoint;

/**
 *
 * @author jiachun.fjc
 */
public class RemotePlacementDriverClient extends AbstractPlacementDriverClient {

    private static final Logger LOG = LoggerFactory.getLogger(RemotePlacementDriverClient.class);

    private String              pdGroupId;
    // 带重试的rpc组件
    private MetadataRpcClient   metadataRpcClient;

    private boolean             started;

    public RemotePlacementDriverClient(long clusterId, String clusterName) {
        super(clusterId, clusterName);
    }

    // 更新group的配置信息、创建rpc组件、刷新路由表
    @Override
    public synchronized boolean init(final PlacementDriverOptions opts) {
        if (this.started) {
            LOG.info("[RemotePlacementDriverClient] already started.");
            return true;
        }
        super.init(opts);
        this.pdGroupId = opts.getPdGroupId();
        if (Strings.isBlank(this.pdGroupId)) {
            throw new IllegalArgumentException("opts.pdGroup id must not be blank");
        }
        final String initialPdServers = opts.getInitialPdServerList();
        if (Strings.isBlank(initialPdServers)) {
            throw new IllegalArgumentException("opts.initialPdServerList must not be blank");
        }
        // 更新group的配置信息(节点列表)
        RouteTable.getInstance().updateConfiguration(this.pdGroupId, initialPdServers);
        // 创建rpc组件
        this.metadataRpcClient = new MetadataRpcClient(super.pdRpcService, 3);
        // 刷新路由表(向pd发送GetClusterInfo请求)
        refreshRouteTable();
        LOG.info("[RemotePlacementDriverClient] start successfully, options: {}.", opts);
        return this.started = true;
    }

    @Override
    public synchronized void shutdown() {
        super.shutdown();
        LOG.info("[RemotePlacementDriverClient] shutdown successfully.");
    }

    @Override
    protected void refreshRouteTable() {
        // 向leader发送GetGlusterInfo请求
        final Cluster cluster = this.metadataRpcClient.getClusterInfo(this.clusterId);
        if (cluster == null) {
            LOG.warn("Cluster info is empty: {}.", this.clusterId);
            return;
        }
        final List<Store> stores = cluster.getStores();
        if (stores == null || stores.isEmpty()) {
            LOG.error("Stores info is empty: {}.", this.clusterId);
            return;
        }
        // 刷新路由表
        for (final Store store : stores) {
            final List<Region> regions = store.getRegions();
            if (regions == null || regions.isEmpty()) {
                LOG.error("Regions info is empty: {} - {}.", this.clusterId, store.getId());
                continue;
            }
            for (final Region region : regions) {
                super.regionRouteTable.addOrUpdateRegion(region);
            }
        }
    }

    @Override
    public Store getStoreMetadata(final StoreEngineOptions opts) {
        final Endpoint selfEndpoint = opts.getServerAddress();
        // remote conf is the preferred
        final Store remoteStore = this.metadataRpcClient.getStoreInfo(this.clusterId, selfEndpoint);
        if (!remoteStore.isEmpty()) {
            final List<Region> regions = remoteStore.getRegions();
            for (final Region region : regions) {
                super.regionRouteTable.addOrUpdateRegion(region);
            }
            return remoteStore;
        }
        // local conf
        final Store localStore = new Store();
        final List<RegionEngineOptions> rOptsList = opts.getRegionEngineOptionsList();
        final List<Region> regionList = Lists.newArrayListWithCapacity(rOptsList.size());
        localStore.setId(remoteStore.getId());
        localStore.setEndpoint(selfEndpoint);
        for (final RegionEngineOptions rOpts : rOptsList) {
            regionList.add(getLocalRegionMetadata(rOpts));
        }
        localStore.setRegions(regionList);
        this.metadataRpcClient.updateStoreInfo(this.clusterId, localStore);
        return localStore;
    }

    @Override
    public Endpoint getPdLeader(final boolean forceRefresh, final long timeoutMillis) {
        // 获取group的leaderId(根据参数forceRefresh决定是否刷新路由表)
        PeerId leader = getLeader(this.pdGroupId, forceRefresh, timeoutMillis);
        if (leader == null && !forceRefresh) {
            // 获取失败，重试且强制刷新路由表
            leader = getLeader(this.pdGroupId, true, timeoutMillis);
        }
        // 未获取到leaderId，抛出异常
        if (leader == null) {
            throw new RouteTableException("no placement driver leader in group: " + this.pdGroupId);
        }
        return new Endpoint(leader.getIp(), leader.getPort());
    }

    public MetadataRpcClient getMetadataRpcClient() {
        return metadataRpcClient;
    }
}
