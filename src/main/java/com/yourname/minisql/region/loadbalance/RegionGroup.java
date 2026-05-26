package com.yourname.minisql.region.loadbalance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 一个 Replica Group（分片），包含一个 Master 和多个 Slave。
 * 写操作由 Master 处理，Slave 只读、负责数据备份。
 */
public class RegionGroup {
    private static final Logger log = LoggerFactory.getLogger(RegionGroup.class);
    
    private final String groupId;
    private volatile LoadBalancer.RegionNode master;
    private final List<LoadBalancer.RegionNode> slaves = new CopyOnWriteArrayList<>();
    
    public RegionGroup(String groupId) {
        this.groupId = groupId;
    }
    
    public String getGroupId() {
        return groupId;
    }
    
    // ---- Master ----
    
    public LoadBalancer.RegionNode getMaster() {
        return master;
    }
    
    public void setMaster(LoadBalancer.RegionNode node) {
        this.master = node;
        log.info("Group '{}': master set to {}", groupId, node);
    }
    
    public void clearMaster() {
        log.info("Group '{}': master cleared (was {})", groupId, master);
        this.master = null;
    }
    
    /**
     * Master 的地址（host:port），没有 master 返回 null
     */
    public String getMasterAddress() {
        return master != null && master.isAvailable() ? master.getAddress() : null;
    }
    
    // ---- Slaves ----
    
    public List<LoadBalancer.RegionNode> getSlaves() {
        return new ArrayList<>(slaves);
    }
    
    public void addSlave(LoadBalancer.RegionNode node) {
        // 防止重复
        boolean exists = slaves.stream()
            .anyMatch(s -> s.getAddress().equals(node.getAddress()));
        if (!exists) {
            slaves.add(node);
            log.info("Group '{}': slave added {}", groupId, node);
        }
    }
    
    public void removeSlave(String address) {
        slaves.removeIf(s -> s.getAddress().equals(address));
        log.info("Group '{}': slave removed {}", groupId, address);
    }
    
    /**
     * 获取所有可用 Slave 地址
     */
    public List<String> getSlaveAddresses() {
        List<String> addrs = new ArrayList<>();
        for (LoadBalancer.RegionNode s : slaves) {
            if (s.isAvailable()) {
                addrs.add(s.getAddress());
            }
        }
        return addrs;
    }
    
    // ---- 移除节点（不区分角色，按 address 查找） ----
    
    /**
     * 按地址移除节点（可能是 master 也可能是 slave）
     * @return true 如果确实移除了某个节点
     */
    public boolean removeByAddress(String address) {
        if (master != null && master.getAddress().equals(address)) {
            clearMaster();
            return true;
        }
        boolean removed = slaves.removeIf(s -> s.getAddress().equals(address));
        if (removed) {
            log.info("Group '{}': node removed by address {}", groupId, address);
        }
        return removed;
    }
    
    // ---- 查询 ----
    
    /**
     * Group 是否有一个可用的 Master
     */
    public boolean hasMaster() {
        return master != null && master.isAvailable();
    }
    
    /**
     * Group 内是否有任何可用节点（Master 或 Slave）
     */
    public boolean hasAvailableNode() {
        if (hasMaster()) return true;
        return slaves.stream().anyMatch(LoadBalancer.RegionNode::isAvailable);
    }
    
    /**
     * 将 Group 信息序列化为可下发给 Client 的字符串格式。
     * 格式: groupId|master=host:port|slave=host:port,host:port,...
     * 如果没有 master, master 部分为空: groupId|master=|slave=...
     */
    public String toClientString() {
        StringBuilder sb = new StringBuilder();
        sb.append(groupId);
        sb.append("|master=");
        if (master != null && master.isAvailable()) {
            sb.append(master.getAddress());
        }
        sb.append("|slaves=");
        List<String> slaveAddrs = getSlaveAddresses();
        sb.append(String.join(",", slaveAddrs));
        return sb.toString();
    }
    
    @Override
    public String toString() {
        return String.format("RegionGroup{id='%s', master=%s, slaves=%d}",
            groupId,
            master != null ? master.getAddress() : "none",
            slaves.size());
    }
}
