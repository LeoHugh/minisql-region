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
    private final String groupId;
    private final String nodePath;        // 分组内的节点路径
    private final String groupRegionsPath; // 分组内的 regions 父路径
    private CuratorCache cache;
    private final Map<String, ZkConfig.RegionData> otherRegions = new ConcurrentHashMap<>();
    
    public RegionRegistry(String host, int port, String groupId) {
        this.zkClient = ZkClientManager.getInstance();
        this.host = host;
        this.port = port;
        this.groupId = groupId;
        this.groupRegionsPath = ZkConfig.getGroupRegionsPath(groupId);
        this.nodePath = groupRegionsPath + "/region-" + port;
    }
    
    /**
     * 注册当前 Region 到 ZK
     */
    public void register() throws Exception {
        // 确保 ZK 客户端已初始化
        if (!zkClient.isInitialized()) {
            zkClient.init();
        }
        
        // 创建节点数据（包含 groupId）
        ZkConfig.RegionData data = new ZkConfig.RegionData(host, port, "online");
        data.setGroupId(groupId);
        byte[] nodeData = JSON.toJSONBytes(data);
        
        // 注册临时节点到分组路径
        zkClient.createEphemeralNode(nodePath, nodeData);
        log.info("Region registered to ZK (group={}): {} -> {}", groupId, nodePath, data);
        
        // 开始监听同组其他 Region
        watchOtherRegions();
    }
    
    /**
     * 监听其他 Region 的变化（用于 Region 间感知，可选）
     */
    private void watchOtherRegions() throws Exception {
        String watchPath = groupRegionsPath;
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
            byte[] dataBytes = node.getData();
            if (dataBytes == null || dataBytes.length == 0) return;
            ZkConfig.RegionData data = ZkConfig.RegionData.fromBytes(dataBytes);
            if (data != null) {
                otherRegions.put(node.getPath(), data);
                log.info("New region online: {} ({})", node.getPath(), data);
            }
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
        
        if (newNode != null && newNode.getData() != null && newNode.getData().length > 0) {
            ZkConfig.RegionData newData = ZkConfig.RegionData.fromBytes(newNode.getData());
            if (newData != null) {
                otherRegions.put(path, newData);
                log.info("Region updated: {} -> {}", path, newData);
            }
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
        data.setGroupId(groupId);
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

    /**
     * 更新 ZK 中的复制元数据（角色 + 复制端口）
     */
    public void updateReplicationInfo(String role, int replicationPort) {
        System.out.println("TEST UPDATE REPLICATION INFO " + role + " " + replicationPort);
        try {
            // 构建带有复制信息的节点数据
            ZkConfig.RegionData data = new ZkConfig.RegionData(host, port, "online");
            data.setRole(role);
            data.setReplicationPort(replicationPort);
            data.setGroupId(groupId);
            
            byte[] nodeData = JSON.toJSONBytes(data);
            
            if (zkClient.getClient().checkExists().forPath(nodePath) != null) {
                zkClient.getClient().setData().forPath(nodePath, nodeData);
                log.info("Updated ZK node with replication info: role={}, replicationPort={}", role, replicationPort);
            } else {
                log.warn("Node {} does not exist in ZK, registering first", nodePath);
                register();
                zkClient.getClient().setData().forPath(nodePath, nodeData);
            }
        } catch (Exception e) {
            log.error("Failed to update ZK replication info", e);
        }
    }

    /**
     * 从 ZK 自动发现同组 Master 的复制地址
     * 只在当前 groupId 对应的路径下查找
     */
    public String discoverMasterReplicationAddress() {
        try {
            log.info("Attempting to discover master replication address from ZooKeeper for group '{}'...", groupId);
            java.util.List<String> children = zkClient.getChildren(groupRegionsPath);
            
            for (String child : children) {
                String path = groupRegionsPath + "/" + child;
                byte[] data = zkClient.getNodeData(path);
                if (data != null && data.length > 0) {
                    ZkConfig.RegionData regionData = ZkConfig.RegionData.fromBytes(data);
                    if (regionData != null && "MASTER".equalsIgnoreCase(regionData.getRole()) 
                            && regionData.getReplicationPort() > 0) {
                        String address = regionData.getHost() + ":" + regionData.getReplicationPort();
                        log.info("Discovered master replication address from ZK (group={}): {}", groupId, address);
                        return address;
                    }
                }
            }
            
            log.warn("No MASTER region found in ZooKeeper for group '{}'", groupId);
        } catch (Exception e) {
            log.error("Failed to discover master from ZooKeeper for group '{}'", groupId, e);
        }
        return null;
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