package com.yourname.minisql.region.master;

import com.yourname.minisql.region.zk.ZkConfig;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Region 负载均衡器
 */
public class RegionLoadBalancer {
    private final List<String> regionAddresses = new CopyOnWriteArrayList<>();
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);
    
    public void addRegion(String address) {
        if (!regionAddresses.contains(address)) {
            regionAddresses.add(address);
        }
    }
    
    public void removeRegion(String address) {
        regionAddresses.remove(address);
    }
    
    public void addRegions(List<String> addresses) {
        for (String addr : addresses) {
            addRegion(addr);
        }
    }
    
    public void updateRegions(List<String> addresses) {
        regionAddresses.clear();
        regionAddresses.addAll(addresses);
    }
    
    /**
     * 轮询策略
     */
    public String getRoundRobin() {
        if (regionAddresses.isEmpty()) {
            return null;
        }
        int index = Math.abs(roundRobinCounter.getAndIncrement() % regionAddresses.size());
        return regionAddresses.get(index);
    }
    
    /**
     * 随机策略
     */
    public String getRandom() {
        if (regionAddresses.isEmpty()) {
            return null;
        }
        int index = (int) (Math.random() * regionAddresses.size());
        return regionAddresses.get(index);
    }
    
    /**
     * 基于表名的哈希策略
     */
    public String getByTableName(String tableName) {
        if (regionAddresses.isEmpty()) {
            return null;
        }
        int hash = Math.abs(tableName.hashCode());
        int index = hash % regionAddresses.size();
        return regionAddresses.get(index);
    }
    
    public int getRegionCount() {
        return regionAddresses.size();
    }
    
    public List<String> getAllRegions() {
        return new java.util.ArrayList<>(regionAddresses);
    }
    
    public boolean isEmpty() {
        return regionAddresses.isEmpty();
    }
    
    public void clear() {
        regionAddresses.clear();
    }
}