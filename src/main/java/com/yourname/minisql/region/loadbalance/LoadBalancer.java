package com.yourname.minisql.region.loadbalance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 无状态负载均衡器 —— 仅根据策略提供选择，不持有任何集群拓扑或元数据状态。
 *
 * 所有拓扑数据（groupMap）由 TopologyManager 管理，
 * 所有元数据映射（tableToGroup）由 MetadataManager 管理。
 * LoadBalancer 的方法接收这些数据作为参数，返回选择结果。
 */
public class LoadBalancer {
    private static final Logger log = LoggerFactory.getLogger(LoadBalancer.class);
    
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);
    private LoadBalanceStrategy strategy = LoadBalanceStrategy.ROUND_ROBIN;
    
    public enum LoadBalanceStrategy {
        ROUND_ROBIN,    // 轮询
        RANDOM,         // 随机
        LEAST_CONN,     // 最少连接
        WEIGHTED,       // 加权轮询
        HASH            // 哈希（同一表到同一 Group）
    }
    
    // ========== 策略配置 ==========
    
    public void setStrategy(LoadBalanceStrategy strategy) {
        this.strategy = strategy;
        log.info("Load balance strategy changed to: {}", strategy);
    }
    
    public LoadBalanceStrategy getCurrentStrategy() {
        return strategy;
    }
    
    // ========== Group 选择（建表时使用） ==========
    
    /**
     * 建表时：从给定的 groupMap 中选择一个有可用 Master 的 RegionGroup。
     *
     * @param groupMap  当前集群拓扑（由 TopologyManager 提供）
     * @param tableName 表名（用于 Hash 策略）
     * @return 选中的 groupId，没有可用 Group 时返回 null
     */
    public String selectGroup(Map<String, RegionGroup> groupMap, String tableName) {
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
     *
     * @param tableName    表名
     * @param tableToGroup table→group 映射（由 MetadataManager 提供）
     * @param groupMap     集群拓扑（由 TopologyManager 提供）
     * @return Group 信息字符串，无可用 Group 时返回 null
     */
    public String getGroupInfoForTable(String tableName,
                                       Map<String, String> tableToGroup,
                                       Map<String, RegionGroup> groupMap) {
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
    public String getGroupInfo(String groupId, Map<String, RegionGroup> groupMap) {
        RegionGroup group = groupMap.get(groupId);
        return group != null ? group.toClientString() : null;
    }
    
    // ========== 统计 / 信息 ==========
    
    public String getStats(Map<String, RegionGroup> groupMap,
                           Map<String, String> tableToGroup) {
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
                sb.append(String.format("    [MASTER] %s: available=%s, conns=%d\n",
                    master.getAddress(), master.isAvailable(), master.getActiveConnections()));
            } else {
                sb.append("    [MASTER] none\n");
            }
            
            for (RegionNode slave : group.getSlaves()) {
                sb.append(String.format("    [SLAVE]  %s: available=%s, conns=%d\n",
                    slave.getAddress(), slave.isAvailable(), slave.getActiveConnections()));
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