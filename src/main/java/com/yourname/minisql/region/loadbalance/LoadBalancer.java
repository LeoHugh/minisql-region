package com.yourname.minisql.region.loadbalance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 智能负载均衡器 - 以 RegionGroup 为粒度进行路由
 * 
 * 核心数据结构：
 *   groupMap     : groupId -> RegionGroup（一个 Master + 多个 Slave）
 *   tableToGroup : tableName -> groupId
 * 
 * CREATE_TABLE 时通过轮询 / Hash 选择一个 RegionGroup；
 * GET_REGION 时向 Client 下发整个 Group 的节点信息。
 */
public class LoadBalancer {
    private static final Logger log = LoggerFactory.getLogger(LoadBalancer.class);
    
    // ---- 核心数据 ----
    private final Map<String, RegionGroup> groupMap = new ConcurrentHashMap<>();
    private final Map<String, String> tableToGroup = new ConcurrentHashMap<>();
    
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);
    private LoadBalanceStrategy strategy = LoadBalanceStrategy.ROUND_ROBIN;
    
    // 统计信息（按 address 粒度）
    private final ConcurrentHashMap<String, RegionStats> statsMap = new ConcurrentHashMap<>();
    
    public enum LoadBalanceStrategy {
        ROUND_ROBIN,    // 轮询
        RANDOM,         // 随机
        LEAST_CONN,     // 最少连接
        WEIGHTED,       // 加权轮询
        HASH            // 哈希（同一表到同一 Group）
    }
    
    /**
     * Region 节点信息（保留原有定义，供 RegionGroup 使用）
     */
    public static class RegionNode {
        private final String id;
        private final String address;
        private volatile int weight = 1;
        private volatile int activeConnections = 0;
        private volatile long lastHeartbeat;
        private volatile boolean available = true;
        
        public RegionNode(String id, String address) {
            this.id = id;
            this.address = address;
            this.lastHeartbeat = System.currentTimeMillis();
        }
        
        public String getId() { return id; }
        public String getAddress() { return address; }
        public int getWeight() { return weight; }
        public void setWeight(int weight) { this.weight = weight; }
        public int getActiveConnections() { return activeConnections; }
        public void incrementConnections() { activeConnections++; }
        public void decrementConnections() { activeConnections--; }
        public boolean isAvailable() { return available; }
        public void setAvailable(boolean available) { this.available = available; }
        public void updateHeartbeat() { lastHeartbeat = System.currentTimeMillis(); }
        
        @Override
        public String toString() {
            return String.format("RegionNode{id='%s', address='%s', weight=%d, conns=%d, available=%s}",
                id, address, weight, activeConnections, available);
        }
    }
    
    /**
     * Region 统计信息
     */
    private static class RegionStats {
        long requestCount = 0;
        long errorCount = 0;
        long avgResponseTime = 0;
        
        void recordRequest(long responseTime, boolean success) {
            requestCount++;
            if (!success) errorCount++;
            avgResponseTime = (avgResponseTime * (requestCount - 1) + responseTime) / requestCount;
        }
    }
    
    // ========== 策略配置 ==========
    
    public void setStrategy(LoadBalanceStrategy strategy) {
        this.strategy = strategy;
        log.info("Load balance strategy changed to: {}", strategy);
    }
    
    public LoadBalanceStrategy getCurrentStrategy() {
        return strategy;
    }
    
    // ========== Group 管理 ==========
    
    /**
     * 添加一个节点到对应的 Group。
     * 如果 Group 不存在则自动创建。
     *
     * @param groupId  分组 ID
     * @param role     "MASTER" / "SLAVE" / "STANDBY"
     * @param nodeId   节点标识（通常 host:port）
     * @param address  节点地址（host:port）
     */
    public void addRegionToGroup(String groupId, String role, String nodeId, String address) {
        RegionGroup group = groupMap.computeIfAbsent(groupId, RegionGroup::new);
        RegionNode node = new RegionNode(nodeId, address);
        
        if ("MASTER".equalsIgnoreCase(role)) {
            group.setMaster(node);
        } else if ("SLAVE".equalsIgnoreCase(role)) {
            group.addSlave(node);
        } else {
            // STANDBY 等其他角色暂时作为 Slave 处理
            group.addSlave(node);
        }
        
        statsMap.putIfAbsent(address, new RegionStats());
        log.info("Added node to group '{}': role={}, address={}", groupId, role, address);
    }
    
    /**
     * 从指定 Group 中移除一个节点（按地址匹配）。
     * 如果 Group 变空则移除 Group。
     */
    public void removeRegionFromGroup(String groupId, String address) {
        RegionGroup group = groupMap.get(groupId);
        if (group != null) {
            group.removeByAddress(address);
            // 如果 Group 已空，移除
            if (!group.hasAvailableNode() && group.getMaster() == null && group.getSlaves().isEmpty()) {
                groupMap.remove(groupId);
                log.info("Group '{}' removed (empty)", groupId);
            }
        }
        statsMap.remove(address);
        log.info("Removed node from group '{}': {}", groupId, address);
    }
    
    /**
     * 从所有 Group 中按地址移除节点（当 groupId 未知时使用）
     */
    public void removeRegion(String address) {
        for (Map.Entry<String, RegionGroup> entry : groupMap.entrySet()) {
            if (entry.getValue().removeByAddress(address)) {
                // 如果 Group 已空，移除
                RegionGroup g = entry.getValue();
                if (g.getMaster() == null && g.getSlaves().isEmpty()) {
                    groupMap.remove(entry.getKey());
                    log.info("Group '{}' removed (empty)", entry.getKey());
                }
            }
        }
        statsMap.remove(address);
        log.info("Removed region: {}", address);
    }
    
    /**
     * 兼容旧接口 —— 将节点添加到默认 Group（无 groupId 信息时的降级处理）
     */
    public void addRegion(String id, String address) {
        addRegionToGroup("default", "STANDBY", id, address);
    }
    
    // ========== 表 -> Group 映射 ==========
    
    /**
     * 获取表对应的 GroupId
     */
    public String getGroupForTable(String tableName) {
        return tableToGroup.get(tableName);
    }
    
    /**
     * 登记 表 -> Group 映射
     */
    public void assignTableToGroup(String tableName, String groupId) {
        tableToGroup.put(tableName, groupId);
        log.info("Table '{}' assigned to group '{}'", tableName, groupId);
    }
    
    /**
     * 移除表映射
     */
    public void removeTableMapping(String tableName) {
        tableToGroup.remove(tableName);
    }
    
    // ========== Group 选择（建表时使用） ==========
    
    /**
     * 建表时：选择一个有可用 Master 的 RegionGroup（轮询 / Hash / 随机）。
     * @param tableName 表名（用于 Hash 策略）
     * @return 选中的 groupId，没有可用 Group 时返回 null
     */
    public String selectGroup(String tableName) {
        List<RegionGroup> availableGroups = groupMap.values().stream()
            .filter(RegionGroup::hasMaster)
            .collect(Collectors.toList());
        
        if (availableGroups.isEmpty()) {
            log.warn("No available group with master");
            return null;
        }
        
        RegionGroup selected;
        switch (strategy) {
            case HASH:
                selected = hashSelectGroup(availableGroups, tableName);
                break;
            case RANDOM:
                selected = randomSelectGroup(availableGroups);
                break;
            case ROUND_ROBIN:
            default:
                selected = roundRobinSelectGroup(availableGroups);
                break;
        }
        return selected != null ? selected.getGroupId() : null;
    }
    
    private RegionGroup roundRobinSelectGroup(List<RegionGroup> groups) {
        int index = Math.abs(roundRobinCounter.getAndIncrement() % groups.size());
        return groups.get(index);
    }
    
    private RegionGroup randomSelectGroup(List<RegionGroup> groups) {
        int index = (int) (Math.random() * groups.size());
        return groups.get(index);
    }
    
    private RegionGroup hashSelectGroup(List<RegionGroup> groups, String tableName) {
        if (tableName == null) return roundRobinSelectGroup(groups);
        int hash = Math.abs(tableName.hashCode());
        int index = hash % groups.size();
        return groups.get(index);
    }
    
    // ========== GET_REGION —— 返回 Group 信息 ==========
    
    /**
     * 根据表名获取其所属 Group 的完整信息字符串（供 Client 解析）。
     * 格式: groupId|master=host:port|slaves=host:port,host:port
     *
     * @param tableName 表名
     * @return Group 信息字符串，无可用 Group 时返回 null
     */
    public String getGroupInfoForTable(String tableName) {
        String groupId = tableToGroup.get(tableName);
        if (groupId == null) {
            return null;
        }
        RegionGroup group = groupMap.get(groupId);
        if (group == null) {
            return null;
        }
        return group.toClientString();
    }
    
    /**
     * 获取指定 Group 的信息字符串
     */
    public String getGroupInfo(String groupId) {
        RegionGroup group = groupMap.get(groupId);
        return group != null ? group.toClientString() : null;
    }
    
    /**
     * 根据表名获取 Master 地址（兼容：写操作总是路由到 Master）
     */
    public String getMasterAddressForTable(String tableName) {
        String groupId = tableToGroup.get(tableName);
        if (groupId == null) return null;
        RegionGroup group = groupMap.get(groupId);
        if (group == null) return null;
        return group.getMasterAddress();
    }
    
    // ========== 兼容旧接口 ==========
    
    /**
     * 获取下一个 Region 地址（兼容旧调用方，返回某个 Group 的 Master 地址）
     */
    public String getNextRegion() {
        return getNextRegion(null);
    }
    
    /**
     * 根据表名获取 Region（兼容旧调用方）
     * 优先返回已映射的 Group Master，否则轮询选一个 Group 的 Master
     */
    public String getNextRegion(String tableName) {
        // 如果已有映射，返回对应 Group 的 Master
        if (tableName != null) {
            String masterAddr = getMasterAddressForTable(tableName);
            if (masterAddr != null) return masterAddr;
        }
        
        // 否则选择一个 Group 的 Master
        List<RegionGroup> groups = groupMap.values().stream()
            .filter(RegionGroup::hasMaster)
            .collect(Collectors.toList());
        
        if (groups.isEmpty()) {
            log.warn("No available region groups");
            return null;
        }
        
        RegionGroup selected = roundRobinSelectGroup(groups);
        return selected.getMasterAddress();
    }
    
    // ========== 可用性管理 ==========
    
    public void markAvailable(String address, boolean available) {
        for (RegionGroup group : groupMap.values()) {
            RegionNode master = group.getMaster();
            if (master != null && master.getAddress().equals(address)) {
                master.setAvailable(available);
                log.info("Region {} in group '{}' is now {}", address, group.getGroupId(), 
                        available ? "available" : "unavailable");
                return;
            }
            for (RegionNode slave : group.getSlaves()) {
                if (slave.getAddress().equals(address)) {
                    slave.setAvailable(available);
                    log.info("Region {} in group '{}' is now {}", address, group.getGroupId(),
                            available ? "available" : "unavailable");
                    return;
                }
            }
        }
    }
    
    public void incrementConnections(String address) {
        findNode(address).ifPresent(RegionNode::incrementConnections);
    }
    
    public void decrementConnections(String address) {
        findNode(address).ifPresent(RegionNode::decrementConnections);
    }
    
    private Optional<RegionNode> findNode(String address) {
        for (RegionGroup group : groupMap.values()) {
            RegionNode master = group.getMaster();
            if (master != null && master.getAddress().equals(address)) {
                return Optional.of(master);
            }
            for (RegionNode slave : group.getSlaves()) {
                if (slave.getAddress().equals(address)) {
                    return Optional.of(slave);
                }
            }
        }
        return Optional.empty();
    }
    
    // ========== 路由表清理 ==========
    
    /**
     * 当某个 Group 的 Master 下线时，清理指向该 Group 的表映射
     */
    public void removeGroupFromRouting(String groupId) {
        List<String> toRemove = new ArrayList<>();
        for (Map.Entry<String, String> entry : tableToGroup.entrySet()) {
            if (entry.getValue().equals(groupId)) {
                toRemove.add(entry.getKey());
            }
        }
        for (String table : toRemove) {
            tableToGroup.remove(table);
            log.info("Removed routing: {} -> group '{}'", table, groupId);
        }
    }
    
    /**
     * 按 Region 地址清理路由表（兼容旧调用方）
     */
    public void removeRegionFromRouting(String address) {
        // 找出包含该地址的 Group
        for (Map.Entry<String, RegionGroup> entry : groupMap.entrySet()) {
            RegionGroup group = entry.getValue();
            RegionNode master = group.getMaster();
            if (master != null && master.getAddress().equals(address)) {
                // Master 下线，清理该 Group 的路由
                removeGroupFromRouting(entry.getKey());
                return;
            }
        }
        // Slave 下线不影响路由
    }
    
    // ========== 统计 / 信息 ==========
    
    public void recordRequest(String address, long responseTime, boolean success) {
        RegionStats stats = statsMap.get(address);
        if (stats != null) {
            stats.recordRequest(responseTime, success);
        }
    }
    
    /**
     * 获取所有 Region 地址（兼容旧接口）
     */
    public List<String> getAllRegions() {
        List<String> result = new ArrayList<>();
        for (RegionGroup group : groupMap.values()) {
            if (group.getMaster() != null && group.getMaster().isAvailable()) {
                result.add(group.getMaster().getAddress());
            }
            for (RegionNode slave : group.getSlaves()) {
                if (slave.isAvailable()) {
                    result.add(slave.getAddress());
                }
            }
        }
        return result;
    }
    
    /**
     * 获取所有 Group
     */
    public Map<String, RegionGroup> getGroupMap() {
        return Collections.unmodifiableMap(groupMap);
    }
    
    /**
     * 获取表到 Group 的映射
     */
    public Map<String, String> getTableToGroup() {
        return Collections.unmodifiableMap(tableToGroup);
    }
    
    public String getStats() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n=== Load Balancer Stats (Group-based) ===\n");
        sb.append("Strategy: ").append(strategy).append("\n");
        sb.append("Groups: ").append(groupMap.size()).append("\n");
        sb.append("Table mappings: ").append(tableToGroup.size()).append("\n\n");
        
        for (Map.Entry<String, RegionGroup> entry : groupMap.entrySet()) {
            RegionGroup group = entry.getValue();
            sb.append("  Group '").append(entry.getKey()).append("':\n");
            
            RegionNode master = group.getMaster();
            if (master != null) {
                RegionStats stats = statsMap.get(master.getAddress());
                sb.append(String.format("    [MASTER] %s: available=%s, conns=%d, requests=%d\n",
                    master.getAddress(), master.isAvailable(), master.getActiveConnections(),
                    stats != null ? stats.requestCount : 0));
            } else {
                sb.append("    [MASTER] none\n");
            }
            
            for (RegionNode slave : group.getSlaves()) {
                RegionStats stats = statsMap.get(slave.getAddress());
                sb.append(String.format("    [SLAVE]  %s: available=%s, conns=%d, requests=%d\n",
                    slave.getAddress(), slave.isAvailable(), slave.getActiveConnections(),
                    stats != null ? stats.requestCount : 0));
            }
        }
        
        if (!tableToGroup.isEmpty()) {
            sb.append("\n  Table -> Group:\n");
            for (Map.Entry<String, String> entry : tableToGroup.entrySet()) {
                sb.append("    ").append(entry.getKey()).append(" -> ").append(entry.getValue()).append("\n");
            }
        }
        
        return sb.toString();
    }
}