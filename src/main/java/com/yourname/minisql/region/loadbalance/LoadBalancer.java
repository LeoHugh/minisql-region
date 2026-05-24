package com.yourname.minisql.region.loadbalance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 智能负载均衡器 - 支持多种策略
 */
public class LoadBalancer {
    private static final Logger log = LoggerFactory.getLogger(LoadBalancer.class);
    
    private final List<RegionNode> regionNodes = new CopyOnWriteArrayList<>();
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);
    private LoadBalanceStrategy strategy = LoadBalanceStrategy.ROUND_ROBIN;
    
    // 统计信息
    private final ConcurrentHashMap<String, RegionStats> statsMap = new ConcurrentHashMap<>();
    
    public enum LoadBalanceStrategy {
        ROUND_ROBIN,    // 轮询
        RANDOM,         // 随机
        LEAST_CONN,     // 最少连接
        WEIGHTED,       // 加权轮询
        HASH            // 哈希（同一表到同一节点）
    }
    
    /**
     * Region 节点信息
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
    
    public void setStrategy(LoadBalanceStrategy strategy) {
        this.strategy = strategy;
        log.info("Load balance strategy changed to: {}", strategy);
    }
    
    public LoadBalanceStrategy getCurrentStrategy() {
        return strategy;
    }
    
    public void addRegion(String id, String address) {
        // 防止重复添加（CuratorCache 事件和显式加载可能同时触发）
        boolean exists = regionNodes.stream()
            .anyMatch(node -> node.getAddress().equals(address));
        if (exists) {
            log.debug("Region already exists, skipping: {} -> {}", id, address);
            return;
        }
        RegionNode node = new RegionNode(id, address);
        regionNodes.add(node);
        statsMap.put(address, new RegionStats());
        log.info("Added region: {} -> {}", id, address);
    }
    
    public void removeRegion(String address) {
        regionNodes.removeIf(node -> node.getAddress().equals(address));
        statsMap.remove(address);
        log.info("Removed region: {}", address);
    }
    
    public void updateRegionWeight(String address, int weight) {
        regionNodes.stream()
            .filter(node -> node.getAddress().equals(address))
            .findFirst()
            .ifPresent(node -> node.setWeight(weight));
        log.info("Updated weight for {}: {}", address, weight);
    }
    
    public void markAvailable(String address, boolean available) {
        regionNodes.stream()
            .filter(node -> node.getAddress().equals(address))
            .findFirst()
            .ifPresent(node -> node.setAvailable(available));
        log.info("Region {} is now {}", address, available ? "available" : "unavailable");
    }
    
    /**
     * 增加指定 Region 的活跃连接计数（在选择 Region 后调用）
     */
    public void incrementConnections(String address) {
        regionNodes.stream()
            .filter(node -> node.getAddress().equals(address))
            .findFirst()
            .ifPresent(RegionNode::incrementConnections);
    }
    
    /**
     * 减少指定 Region 的活跃连接计数（在请求完成后调用）
     */
    public void decrementConnections(String address) {
        regionNodes.stream()
            .filter(node -> node.getAddress().equals(address))
            .findFirst()
            .ifPresent(RegionNode::decrementConnections);
    }
    
    /**
     * 获取下一个 Region
     */
    public String getNextRegion() {
        return getNextRegion(null);
    }
    
    /**
     * 根据表名获取 Region（用于哈希策略）
     */
    public String getNextRegion(String tableName) {
        List<RegionNode> availableNodes = regionNodes.stream()
            .filter(RegionNode::isAvailable)
            .collect(Collectors.toList());
        
        if (availableNodes.isEmpty()) {
            log.warn("No available region nodes");
            return null;
        }
        
        switch (strategy) {
            case RANDOM:
                return randomSelect(availableNodes);
            case LEAST_CONN:
                return leastConnSelect(availableNodes);
            case WEIGHTED:
                return weightedSelect(availableNodes);
            case HASH:
                return hashSelect(availableNodes, tableName);
            case ROUND_ROBIN:
            default:
                return roundRobinSelect(availableNodes);
        }
    }
    
    private String roundRobinSelect(List<RegionNode> nodes) {
        int index = Math.abs(roundRobinCounter.getAndIncrement() % nodes.size());
        return nodes.get(index).getAddress();
    }
    
    private String randomSelect(List<RegionNode> nodes) {
        int index = (int) (Math.random() * nodes.size());
        return nodes.get(index).getAddress();
    }
    
    private String leastConnSelect(List<RegionNode> nodes) {
        return nodes.stream()
            .min((a, b) -> Integer.compare(a.getActiveConnections(), b.getActiveConnections()))
            .map(RegionNode::getAddress)
            .orElse(null);
    }
    
    private String weightedSelect(List<RegionNode> nodes) {
        int totalWeight = nodes.stream().mapToInt(RegionNode::getWeight).sum();
        if (totalWeight == 0) return roundRobinSelect(nodes);
        
        int random = (int) (Math.random() * totalWeight);
        int currentWeight = 0;
        for (RegionNode node : nodes) {
            currentWeight += node.getWeight();
            if (random < currentWeight) {
                return node.getAddress();
            }
        }
        return nodes.get(0).getAddress();
    }
    
    private String hashSelect(List<RegionNode> nodes, String tableName) {
        if (tableName == null) {
            return roundRobinSelect(nodes);
        }
        int hash = Math.abs(tableName.hashCode());
        int index = hash % nodes.size();
        return nodes.get(index).getAddress();
    }
    
    /**
     * 记录请求统计（用于自适应负载均衡）
     */
    public void recordRequest(String address, long responseTime, boolean success) {
        RegionStats stats = statsMap.get(address);
        if (stats != null) {
            stats.recordRequest(responseTime, success);
        }
    }
    
    /**
     * 获取当前所有 Region
     */
    public List<String> getAllRegions() {
        return regionNodes.stream()
            .filter(RegionNode::isAvailable)
            .map(RegionNode::getAddress)
            .collect(Collectors.toList());
    }
    
    /**
     * 获取统计信息
     */
    public String getStats() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n=== Load Balancer Stats ===\n");
        for (RegionNode node : regionNodes) {
            RegionStats stats = statsMap.get(node.getAddress());
            sb.append(String.format("  %s: available=%s, weight=%d, conns=%d, requests=%d, errors=%d\n",
                node.getAddress(), node.isAvailable(), node.getWeight(),
                node.getActiveConnections(),
                stats != null ? stats.requestCount : 0,
                stats != null ? stats.errorCount : 0));
        }
        return sb.toString();
    }
}