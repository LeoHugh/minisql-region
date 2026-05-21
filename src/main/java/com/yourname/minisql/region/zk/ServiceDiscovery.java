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
            byte[] data = node.getData();
            if (data != null && data.length > 0) {
                ZkConfig.RegionData regionData = parseRegionData(data);
                if (regionData != null) {
                    onlineRegions.add(regionData);
                    log.info("Region online: {} -> {}", nodeName, regionData);
                    notifyListeners(regionData, true);
                } else {
                    log.warn("Failed to parse region data from node: {}, data: {}", 
                             nodeName, new String(data));
                }
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
            } else {
                // 如果找不到，尝试从路径中提取端口
                try {
                    String portStr = nodeName.replaceAll("\\D+", "");
                    if (!portStr.isEmpty()) {
                        int port = Integer.parseInt(portStr);
                        ZkConfig.RegionData fallback = new ZkConfig.RegionData();
                        fallback.setHost("localhost");
                        fallback.setPort(port);
                        fallback.setStatus("offline");
                        log.info("Region offline (fallback): {} -> {}", nodeName, fallback);
                        notifyListeners(fallback, false);
                    }
                } catch (Exception e) {
                    log.warn("Failed to extract port from node: {}", nodeName);
                }
            }
        })
        .forChanges((oldNode, newNode) -> {
            if (newNode != null && newNode.getData() != null && newNode.getData().length > 0) {
                ZkConfig.RegionData newData = parseRegionData(newNode.getData());
                if (newData != null) {
                    // 更新列表中对应的 Region
                    boolean found = false;
                    for (int i = 0; i < onlineRegions.size(); i++) {
                        if (onlineRegions.get(i).getPort() == newData.getPort()) {
                            onlineRegions.set(i, newData);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        onlineRegions.add(newData);
                    }
                    log.info("Region updated: {}", newData);
                    notifyListeners(newData, true);
                }
            }
        })
        .build()
    );
    cache.start();
    
    log.info("Started watching regions at: {}", watchPath);
}

/**
 * 解析 Region 数据，兼容 JSON 和纯地址格式
 * @param data 原始字节数据
 * @return RegionData 对象，解析失败返回 null
 */
private ZkConfig.RegionData parseRegionData(byte[] data) {
    if (data == null || data.length == 0) {
        return null;
    }
    
    String dataStr = new String(data);
    log.debug("Parsing region data: {}", dataStr);
    
    // 方法1：尝试解析 JSON
    if (dataStr.trim().startsWith("{")) {
        try {
            ZkConfig.RegionData regionData = JSON.parseObject(dataStr, ZkConfig.RegionData.class);
            if (regionData != null && regionData.getPort() > 0) {
                log.debug("Parsed as JSON: {}", regionData);
                return regionData;
            }
        } catch (Exception e) {
            log.debug("Failed to parse as JSON: {}", e.getMessage());
        }
    }
    
    // 方法2：尝试解析为地址格式 "host:port"
    if (dataStr.contains(":")) {
        try {
            String[] parts = dataStr.split(":");
            if (parts.length >= 2) {
                String host = parts[0];
                int port = Integer.parseInt(parts[1].trim());
                ZkConfig.RegionData regionData = new ZkConfig.RegionData();
                regionData.setHost(host);
                regionData.setPort(port);
                regionData.setStatus("online");
                regionData.setTimestamp(System.currentTimeMillis());
                log.debug("Parsed as address: {}", regionData);
                return regionData;
            }
        } catch (NumberFormatException e) {
            log.debug("Failed to parse port from: {}", dataStr);
        }
    }
    
    // 方法3：尝试从 JSON-like 字符串中提取（兼容损坏的 JSON）
    try {
        // 尝试提取 host
        String host = extractValue(dataStr, "host");
        String portStr = extractValue(dataStr, "port");
        if (host != null && portStr != null) {
            int port = Integer.parseInt(portStr);
            ZkConfig.RegionData regionData = new ZkConfig.RegionData();
            regionData.setHost(host);
            regionData.setPort(port);
            regionData.setStatus(extractValue(dataStr, "status"));
            if (regionData.getStatus() == null) {
                regionData.setStatus("online");
            }
            log.debug("Extracted from JSON-like: {}", regionData);
            return regionData;
        }
    } catch (Exception e) {
        log.debug("Failed to extract from JSON-like: {}", e.getMessage());
    }
    
    log.warn("Could not parse region data: {}", dataStr);
    return null;
}

/**
 * 从 JSON-like 字符串中提取值
 */
private String extractValue(String json, String key) {
    String searchKey = "\"" + key + "\"";
    int keyIndex = json.indexOf(searchKey);
    if (keyIndex < 0) {
        return null;
    }
    
    int colonIndex = json.indexOf(":", keyIndex);
    if (colonIndex < 0) {
        return null;
    }
    
    int valueStart = colonIndex + 1;
    while (valueStart < json.length() && (json.charAt(valueStart) == ' ' || json.charAt(valueStart) == '"')) {
        valueStart++;
    }
    
    int valueEnd = valueStart;
    if (json.charAt(valueStart) == '"') {
        valueStart++;
        valueEnd = json.indexOf("\"", valueStart);
    } else {
        while (valueEnd < json.length() && json.charAt(valueEnd) != ',' && json.charAt(valueEnd) != '}') {
            valueEnd++;
        }
    }
    
    if (valueEnd > valueStart) {
        return json.substring(valueStart, valueEnd);
    }
    
    return null;
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