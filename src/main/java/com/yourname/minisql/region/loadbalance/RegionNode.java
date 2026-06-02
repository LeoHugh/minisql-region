package com.yourname.minisql.region.loadbalance;

/**
 * Region 节点信息（一个 Region 进程在集群中的逻辑表示）。
 * 之前作为 LoadBalancer 的内部类，重构后提取为独立顶层类，
 * 供 RegionGroup、TopologyManager、LoadBalancer 等共同使用。
 */
public class RegionNode {
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
