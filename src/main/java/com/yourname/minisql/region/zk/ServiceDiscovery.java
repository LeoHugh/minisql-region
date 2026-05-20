package com.yourname.minisql.region.zk;

import com.alibaba.fastjson2.JSON;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Master 端：服务发现，监听 Region 变化
 */
public class ServiceDiscovery implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(ServiceDiscovery.class);
    
    private final ZkClientManager zkClient;
    private CuratorCache cache;
    private final List<RegionChangeListener> listeners = new CopyOnWriteArrayList<>();
    private final List<ZkConfig.RegionData> onlineRegions = new CopyOnWriteArrayList<>();
    private final AtomicInteger roundRobinIndex = new AtomicInteger(0);
    
    public ServiceDiscovery() {
        this.zkClient = ZkClientManager.getInstance();
    }
    
    /**
     * 初始化并开始监听
     */
    public void start() throws Exception {
        if (!zkClient.isInitialized()) {
            zkClient.init();
        }
        
        // 加载已存在的 Region
        loadExistingRegions();
        
        // 监听 Region 变化
        watchRegions();
        
        log.info("ServiceDiscovery started");
    }
    
    /**
     * 加载已存在的 Region
     */
    private void loadExistingRegions() throws Exception {
        List<String> children = zkClient.getChildren(ZkConfig.ZK_REGIONS_PATH);
        for (String child : children) {
            String path = ZkConfig.ZK_REGIONS_PATH + "/" + child;
            byte[] data = zkClient.getNodeData(path);
            if (data != null && data.length > 0) {
                ZkConfig.RegionData regionData = JSON.parseObject(data, ZkConfig.RegionData.class);
                onlineRegions.add(regionData);
                log.info("Loaded existing region: {} -> {}", child, regionData);
            }
        }
        log.info("Loaded {} existing regions", onlineRegions.size());
    }
    
    /**
     * 监听 Region 节点变化
     */
    private void watchRegions() throws Exception {
        CuratorFramework client = zkClient.getClient();
        String watchPath = ZkConfig.ZK_REGIONS_PATH;
        
        cache = CuratorCache.build(client, watchPath);
        cache.listenable().addListener(CuratorCacheListener.builder()
            .forCreates(node -> {
                String path = node.getPath();
                String nodeName = path.substring(path.lastIndexOf('/') + 1);
                if (node.getData() != null) {
                    ZkConfig.RegionData regionData = JSON.parseObject(node.getData(), ZkConfig.RegionData.class);
                    onlineRegions.add(regionData);
                    log.info("Region online: {} -> {}", nodeName, regionData);
                    notifyListeners(regionData, true);
                }
            })
            .forDeletes(node -> {
                String path = node.getPath();
                String nodeName = path.substring(path.lastIndexOf('/') + 1);
                // 需要从数据中获取地址，这里简化处理
                ZkConfig.RegionData removed = onlineRegions.stream()
                    .filter(r -> path.contains(String.valueOf(r.getPort())))
                    .findFirst()
                    .orElse(null);
                if (removed != null) {
                    onlineRegions.remove(removed);
                    log.info("Region offline: {} -> {}", nodeName, removed);
                    notifyListeners(removed, false);
                }
            })
            .forChanges((oldNode, newNode) -> {
                if (newNode != null && newNode.getData() != null) {
                    ZkConfig.RegionData newData = JSON.parseObject(newNode.getData(), ZkConfig.RegionData.class);
                    // 更新列表中对应的 Region
                    for (int i = 0; i < onlineRegions.size(); i++) {
                        if (onlineRegions.get(i).getPort() == newData.getPort()) {
                            onlineRegions.set(i, newData);
                            break;
                        }
                    }
                    log.info("Region updated: {}", newData);
                }
            })
            .build()
        );
        cache.start();
        
        log.info("Started watching regions at: {}", watchPath);
    }
    
    /**
     * 获取所有在线 Region
     */
    public List<ZkConfig.RegionData> getOnlineRegions() {
        return new ArrayList<>(onlineRegions);
    }
    
    /**
     * 获取所有在线 Region 的地址
     */
    public List<String> getOnlineRegionAddresses() {
        return onlineRegions.stream()
            .map(ZkConfig.RegionData::getAddress)
            .collect(Collectors.toList());
    }
    
    /**
     * 轮询策略获取下一个 Region 地址
     */
    public String getNextRegionRoundRobin() {
        if (onlineRegions.isEmpty()) {
            return null;
        }
        int index = Math.abs(roundRobinIndex.getAndIncrement() % onlineRegions.size());
        return onlineRegions.get(index).getAddress();
    }
    
    /**
     * 随机策略获取 Region 地址
     */
    public String getRandomRegion() {
        if (onlineRegions.isEmpty()) {
            return null;
        }
        int index = (int) (Math.random() * onlineRegions.size());
        return onlineRegions.get(index).getAddress();
    }
    
    /**
     * 根据表名获取 Region（简单策略：取模）
     */
    public String getRegionByTable(String tableName) {
        if (onlineRegions.isEmpty()) {
            return null;
        }
        // 使用表名的 hash 值取模选择 Region
        int hash = Math.abs(tableName.hashCode());
        int index = hash % onlineRegions.size();
        return onlineRegions.get(index).getAddress();
    }
    
    /**
     * 添加监听器
     */
    public void addListener(RegionChangeListener listener) {
        listeners.add(listener);
    }
    
    /**
     * 移除监听器
     */
    public void removeListener(RegionChangeListener listener) {
        listeners.remove(listener);
    }
    
    private void notifyListeners(ZkConfig.RegionData region, boolean online) {
        for (RegionChangeListener listener : listeners) {
            try {
                if (online) {
                    listener.onRegionOnline(region);
                } else {
                    listener.onRegionOffline(region);
                }
            } catch (Exception e) {
                log.error("Error notifying listener", e);
            }
        }
    }
    
    /**
     * Region 变化监听器接口
     */
    public interface RegionChangeListener {
        void onRegionOnline(ZkConfig.RegionData region);
        void onRegionOffline(ZkConfig.RegionData region);
    }
    
    @Override
    public void close() throws IOException {
        if (cache != null) {
            cache.close();
        }
    }
}