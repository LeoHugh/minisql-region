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
 * Master 端：服务发现，监听所有 Group 下的 Region 变化。
 * 
 * ZK 路径结构：
 *   /minisql/groups/<groupId>/regions/region-<port>
 * 
 * 当节点上线 / 下线 / 数据变更时，提取 groupId 和 role 信息，
 * 然后通知 RegionChangeListener（LoadBalancer 通过监听器同步数据）。
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
        
        // 确保 groups 根路径存在
        zkClient.ensurePathExists(ZkConfig.ZK_GROUPS_PATH);
        
        // 加载所有已存在的 Group 节点
        loadExistingRegions();
        
        // 监听 /minisql/groups 下的整棵树（递归监听所有 Group）
        watchAllGroups();
        
        log.info("ServiceDiscovery started (group-aware)");
    }
    
    /**
     * 加载所有已存在的 Region（遍历所有 Group）
     */
    private void loadExistingRegions() throws Exception {
        String groupsPath = ZkConfig.ZK_GROUPS_PATH;
        
        List<String> groups;
        try {
            groups = zkClient.getChildren(groupsPath);
        } catch (Exception e) {
            log.warn("No groups path yet: {}", e.getMessage());
            return;
        }
        
        for (String groupId : groups) {
            String regionsPath = ZkConfig.getGroupRegionsPath(groupId);
            List<String> children;
            try {
                children = zkClient.getChildren(regionsPath);
            } catch (Exception e) {
                log.debug("No regions under group '{}': {}", groupId, e.getMessage());
                continue;
            }
            
            for (String child : children) {
                String nodePath = regionsPath + "/" + child;
                byte[] data = zkClient.getNodeData(nodePath);
                if (data != null && data.length > 0) {
                    ZkConfig.RegionData regionData = parseRegionData(data);
                    if (regionData != null) {
                        // 确保 groupId 已填充
                        if (regionData.getGroupId() == null || regionData.getGroupId().isEmpty()) {
                            regionData.setGroupId(groupId);
                        }
                        onlineRegions.add(regionData);
                        log.info("Loaded existing region: group='{}', node='{}', role='{}', addr='{}'",
                                groupId, child, regionData.getRole(), regionData.getAddress());
                        notifyListeners(regionData, true);
                    }
                }
            }
        }
        log.info("Loaded {} existing regions across all groups", onlineRegions.size());
    }
    
    /**
     * 递归监听 /minisql/groups 下所有节点变化
     */
    private void watchAllGroups() throws Exception {
        CuratorFramework client = zkClient.getClient();
        String watchPath = ZkConfig.ZK_GROUPS_PATH;
        
        cache = CuratorCache.build(client, watchPath);
        cache.listenable().addListener(CuratorCacheListener.builder()
            .forCreates(node -> {
                String path = node.getPath();
                // 只处理 regions 下的叶子节点（region-xxxx）
                if (!isRegionNodePath(path)) return;
                
                byte[] data = node.getData();
                if (data != null && data.length > 0) {
                    ZkConfig.RegionData regionData = parseRegionData(data);
                    if (regionData != null) {
                        // 从路径中提取 groupId
                        String groupId = extractGroupIdFromPath(path);
                        if (regionData.getGroupId() == null || regionData.getGroupId().isEmpty()) {
                            regionData.setGroupId(groupId);
                        }
                        
                        // 防重
                        boolean exists = onlineRegions.stream()
                            .anyMatch(r -> r.getHost().equals(regionData.getHost())
                                        && r.getPort() == regionData.getPort());
                        if (!exists) {
                            onlineRegions.add(regionData);
                            log.info("Region online: group='{}', role='{}', addr='{}'",
                                    groupId, regionData.getRole(), regionData.getAddress());
                            notifyListeners(regionData, true);
                        }
                    }
                }
            })
            .forDeletes(node -> {
                String path = node.getPath();
                if (!isRegionNodePath(path)) return;
                
                String groupId = extractGroupIdFromPath(path);
                
                // 从在线列表中查找并移除
                ZkConfig.RegionData removed = onlineRegions.stream()
                    .filter(r -> path.contains(String.valueOf(r.getPort())))
                    .findFirst()
                    .orElse(null);
                    
                if (removed != null) {
                    onlineRegions.remove(removed);
                    if (removed.getGroupId() == null || removed.getGroupId().isEmpty()) {
                        removed.setGroupId(groupId);
                    }
                    log.info("Region offline: group='{}', role='{}', addr='{}'",
                            groupId, removed.getRole(), removed.getAddress());
                    notifyListeners(removed, false);
                } else {
                    // 降级：从路径中提取端口
                    try {
                        String nodeName = path.substring(path.lastIndexOf('/') + 1);
                        String portStr = nodeName.replaceAll("\\D+", "");
                        if (!portStr.isEmpty()) {
                            int port = Integer.parseInt(portStr);
                            ZkConfig.RegionData fallback = new ZkConfig.RegionData();
                            fallback.setHost("localhost");
                            fallback.setPort(port);
                            fallback.setStatus("offline");
                            fallback.setGroupId(groupId);
                            log.info("Region offline (fallback): group='{}', port={}", groupId, port);
                            notifyListeners(fallback, false);
                        }
                    } catch (Exception e) {
                        log.warn("Failed to extract port from path: {}", path);
                    }
                }
            })
            .forChanges((oldNode, newNode) -> {
                if (newNode == null || newNode.getData() == null || newNode.getData().length == 0) return;
                String path = newNode.getPath();
                if (!isRegionNodePath(path)) return;
                
                ZkConfig.RegionData newData = parseRegionData(newNode.getData());
                if (newData != null) {
                    String groupId = extractGroupIdFromPath(path);
                    if (newData.getGroupId() == null || newData.getGroupId().isEmpty()) {
                        newData.setGroupId(groupId);
                    }
                    
                    // 更新列表
                    boolean found = false;
                    for (int i = 0; i < onlineRegions.size(); i++) {
                        ZkConfig.RegionData existing = onlineRegions.get(i);
                        if (existing.getHost().equals(newData.getHost()) && existing.getPort() == newData.getPort()) {
                            onlineRegions.set(i, newData);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        onlineRegions.add(newData);
                    }
                    log.info("Region updated: group='{}', role='{}', addr='{}'",
                            groupId, newData.getRole(), newData.getAddress());
                    notifyListeners(newData, true);
                }
            })
            .build()
        );
        cache.start();
        
        log.info("Started watching all groups at: {}", watchPath);
    }
    
    /**
     * 判断一个 ZK 路径是否为 region 节点路径
     * 例: /minisql/groups/group1/regions/region-8888 -> true
     *     /minisql/groups/group1/regions             -> false
     *     /minisql/groups/group1                     -> false
     */
    private boolean isRegionNodePath(String path) {
        // 路径格式: .../groups/<groupId>/regions/<regionNode>
        return path.contains("/regions/") && !path.endsWith("/regions");
    }
    
    /**
     * 从 ZK 路径中提取 groupId
     * 例: /minisql/groups/group1/regions/region-8888 -> group1
     */
    private String extractGroupIdFromPath(String path) {
        // 路径: .../groups/<groupId>/regions/...
        int groupsIdx = path.indexOf("/groups/");
        if (groupsIdx < 0) return "unknown";
        
        String afterGroups = path.substring(groupsIdx + "/groups/".length());
        int slashIdx = afterGroups.indexOf('/');
        if (slashIdx < 0) return afterGroups;
        return afterGroups.substring(0, slashIdx);
    }
    
    /**
     * 解析 Region 数据，兼容 JSON 和纯地址格式
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
        
        // 方法3：尝试从 JSON-like 字符串中提取
        try {
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
                String role = extractValue(dataStr, "role");
                if (role != null) {
                    regionData.setRole(role);
                }
                String gid = extractValue(dataStr, "groupId");
                if (gid != null) {
                    regionData.setGroupId(gid);
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
    
    // ========== 查询接口 ==========
    
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
        int hash = Math.abs(tableName.hashCode());
        int index = hash % onlineRegions.size();
        return onlineRegions.get(index).getAddress();
    }
    
    // ========== 监听器管理 ==========
    
    public void addListener(RegionChangeListener listener) {
        listeners.add(listener);
    }
    
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