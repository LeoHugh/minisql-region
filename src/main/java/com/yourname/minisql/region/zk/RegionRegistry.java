package com.yourname.minisql.region.zk;

import com.alibaba.fastjson2.JSON;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Region 端：向 ZK 注册自身，并监听其他 Region 的变化
 */
public class RegionRegistry implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(RegionRegistry.class);
    
    private final ZkClientManager zkClient;
    private final String host;
    private final int port;
    private final String nodePath;
    private CuratorCache cache;
    private final Map<String, ZkConfig.RegionData> otherRegions = new ConcurrentHashMap<>();
    
    public RegionRegistry(String host, int port) {
        this.zkClient = ZkClientManager.getInstance();
        this.host = host;
        this.port = port;
        this.nodePath = ZkConfig.ZK_REGIONS_PATH + "/region-" + port;
    }
    
    /**
     * 注册当前 Region 到 ZK
     */
    public void register() throws Exception {
        // 确保 ZK 客户端已初始化
        if (!zkClient.isInitialized()) {
            zkClient.init();
        }
        
        // 创建节点数据
        ZkConfig.RegionData data = new ZkConfig.RegionData(host, port, "online");
        byte[] nodeData = JSON.toJSONBytes(data);
        
        // 注册临时节点
        zkClient.createEphemeralNode(nodePath, nodeData);
        log.info("Region registered to ZK: {} -> {}", nodePath, data);
        
        // 开始监听其他 Region
        watchOtherRegions();
    }
    
    /**
     * 监听其他 Region 的变化（用于 Region 间感知，可选）
     */
    private void watchOtherRegions() throws Exception {
        String watchPath = ZkConfig.ZK_REGIONS_PATH;
        CuratorFramework client = zkClient.getClient();
        
        cache = CuratorCache.build(client, watchPath);
        cache.listenable().addListener(CuratorCacheListener.builder()
            .forChanges((oldNode, newNode) -> {
                String path = newNode != null ? newNode.getPath() : 
                             (oldNode != null ? oldNode.getPath() : null);
                if (path == null || path.equals(nodePath)) {
                    return;
                }
                handleRegionChange(oldNode, newNode);
            })
            .forCreates(node -> {
                if (!node.getPath().equals(nodePath)) {
                    handleRegionAdd(node);
                }
            })
            .forDeletes(node -> {
                if (!node.getPath().equals(nodePath)) {
                    handleRegionRemove(node);
                }
            })
            .build()
        );
        cache.start();
        
        log.info("Started watching other regions at: {}", watchPath);
    }
    
    private void handleRegionAdd(ChildData node) {
        try {
            ZkConfig.RegionData data = JSON.parseObject(node.getData(), ZkConfig.RegionData.class);
            otherRegions.put(node.getPath(), data);
            log.info("New region online: {} ({})", node.getPath(), data);
        } catch (Exception e) {
            log.error("Failed to parse region data", e);
        }
    }
    
    private void handleRegionRemove(ChildData node) {
        ZkConfig.RegionData removed = otherRegions.remove(node.getPath());
        log.info("Region offline: {} ({})", node.getPath(), removed);
    }
    
    private void handleRegionChange(ChildData oldNode, ChildData newNode) {
        String path = newNode != null ? newNode.getPath() : 
                     (oldNode != null ? oldNode.getPath() : null);
        if (path == null) return;
        
        if (newNode != null && newNode.getData() != null) {
            ZkConfig.RegionData newData = JSON.parseObject(newNode.getData(), ZkConfig.RegionData.class);
            otherRegions.put(path, newData);
            log.info("Region updated: {} -> {}", path, newData);
        } else if (oldNode != null) {
            otherRegions.remove(path);
            log.info("Region removed: {}", path);
        }
    }
    
    /**
     * 注销当前 Region
     */
    public void unregister() throws Exception {
        zkClient.deleteNode(nodePath);
        log.info("Region unregistered: {}", nodePath);
    }
    
    /**
     * 更新 Region 状态
     */
    public void updateStatus(String status) throws Exception {
        ZkConfig.RegionData data = new ZkConfig.RegionData(host, port, status);
        byte[] nodeData = JSON.toJSONBytes(data);
        
        if (zkClient.getClient().checkExists().forPath(nodePath) != null) {
            zkClient.getClient().setData().forPath(nodePath, nodeData);
            log.info("Region status updated: {} -> {}", nodePath, status);
        } else {
            register();
        }
    }
    
    /**
     * 获取所有其他 Region 的地址
     */
    public java.util.List<String> getOtherRegionAddresses() {
        return otherRegions.values().stream()
            .map(ZkConfig.RegionData::getAddress)
            .collect(java.util.stream.Collectors.toList());
    }
    
    @Override
    public void close() throws IOException {
        try {
            if (cache != null) {
                cache.close();
            }
            unregister();
        } catch (Exception e) {
            log.error("Error closing RegionRegistry", e);
        }
    }
}