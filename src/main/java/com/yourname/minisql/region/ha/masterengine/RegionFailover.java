package com.yourname.minisql.region.ha.masterengine;


import com.yourname.minisql.region.zk.ServiceDiscovery;
import com.yourname.minisql.region.zk.ZkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Region 故障检测与自动切换
 * 
 * 通过实现 ServiceDiscovery.RegionChangeListener 复用 ServiceDiscovery 的 ZK 监听，
 * 避免重复创建 CuratorCache。自身专注于 TCP 主动健康检查（可在 ZK 临时节点过期前
 * 提前发现 Region 进程假死）。
 */
public class RegionFailover implements ServiceDiscovery.RegionChangeListener {
    private static final Logger log = LoggerFactory.getLogger(RegionFailover.class);
    
    private final Map<String, RegionHealth> regionHealthMap = new ConcurrentHashMap<>();
    private final ScheduledExecutorService healthChecker;
    private final FailoverListener failoverListener;
    
    // 健康检查配置
    private static final int HEALTH_CHECK_INTERVAL_SEC = 5;
    private static final int MAX_RETRY_COUNT = 3;
    private static final int RETRY_TIMEOUT_MS = 3000;
    
    public interface FailoverListener {
        void onRegionFailed(String regionId, String address);      // Region 故障
        void onRegionRecovered(String regionId, String address);   // Region 恢复
        void onFailoverCompleted(String fromRegion, String toRegion); // 切换完成
    }
    
    public RegionFailover(FailoverListener listener) {
        this.failoverListener = listener;
        this.healthChecker = Executors.newSingleThreadScheduledExecutor();
    }
    
    /**
     * 启动健康检查定时任务
     */
    public void start() {
        healthChecker.scheduleAtFixedRate(this::healthCheck, 
            HEALTH_CHECK_INTERVAL_SEC, HEALTH_CHECK_INTERVAL_SEC, TimeUnit.SECONDS);
        log.info("Region failover started (health check interval: {}s)", HEALTH_CHECK_INTERVAL_SEC);
    }
    
    public void stop() {
        healthChecker.shutdown();
        regionHealthMap.clear();
        log.info("Region failover stopped");
    }
    
    // ========== ServiceDiscovery.RegionChangeListener 实现 ==========
    
    /**
     * 当 ServiceDiscovery 感知到 Region 上线时，自动注册到健康检查列表
     */
    @Override
    public void onRegionOnline(ZkConfig.RegionData region) {
        String regionId = region.getHost() + ":" + region.getPort();
        String address = region.getAddress();
        registerRegion(regionId, address);
    }
    
    /**
     * 当 ServiceDiscovery 感知到 Region 下线时（ZK 节点被删除），
     * 从健康检查列表中移除。
     * 注：此处不再触发 onRegionFailed，因为 MasterServer 的 ServiceDiscovery 监听器
     * 已经通过 loadBalancer.removeRegion() 处理了路由移除。
     */
    @Override
    public void onRegionOffline(ZkConfig.RegionData region) {
        String regionId = region.getHost() + ":" + region.getPort();
        RegionHealth health = regionHealthMap.remove(regionId);
        if (health != null) {
            log.info("Region removed from health check: {} ({})", regionId, health.getAddress());
        }
    }
    
    // ========== 主动 TCP 健康检查（RegionFailover 的核心价值） ==========
    
    private void healthCheck() {
        for (Map.Entry<String, RegionHealth> entry : regionHealthMap.entrySet()) {
            String regionId = entry.getKey();
            RegionHealth health = entry.getValue();
            
            boolean alive = checkRegionAlive(health.getAddress());
            if (!alive) {
                int failedCount = health.incrementFailedCount();
                if (failedCount >= MAX_RETRY_COUNT && health.isHealthy()) {
                    health.setHealthy(false);
                    log.error("Region {} failed after {} retries: {}", 
                             regionId, failedCount, health.getAddress());
                    if (failoverListener != null) {
                        failoverListener.onRegionFailed(regionId, health.getAddress());
                    }
                }
            } else {
                if (!health.isHealthy()) {
                    health.setHealthy(true);
                    log.info("Region {} recovered: {}", regionId, health.getAddress());
                    if (failoverListener != null) {
                        failoverListener.onRegionRecovered(regionId, health.getAddress());
                    }
                }
                health.resetFailedCount();
            }
        }
    }
    
    private boolean checkRegionAlive(String address) {
        String[] parts = address.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        
        try (java.net.Socket socket = new java.net.Socket()) {
            socket.connect(new java.net.InetSocketAddress(host, port), RETRY_TIMEOUT_MS);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * 注册 Region 到健康检查列表
     */
    public void registerRegion(String regionId, String address) {
        if (regionHealthMap.containsKey(regionId)) {
            log.debug("Region already registered for health check: {}", regionId);
            return;
        }
        regionHealthMap.put(regionId, new RegionHealth(regionId, address));
        log.info("Registered region for health check: {} -> {}", regionId, address);
    }
    
    /**
     * 获取当前监控的 Region 数量
     */
    public int getMonitoredRegionCount() {
        return regionHealthMap.size();
    }
    
    private String extractRegionId(String nodePath) {
        int lastSlash = nodePath.lastIndexOf('/');
        if (lastSlash >= 0) {
            return nodePath.substring(lastSlash + 1);
        }
        return null;
    }
    
    private static class RegionHealth {
        private final String regionId;
        private final String address;
        private volatile boolean healthy = true;
        private int failedCount = 0;
        
        public RegionHealth(String regionId, String address) {
            this.regionId = regionId;
            this.address = address;
        }
        
        public String getAddress() { return address; }
        public boolean isHealthy() { return healthy; }
        public void setHealthy(boolean healthy) { this.healthy = healthy; }
        
        public int incrementFailedCount() { return ++failedCount; }
        public void resetFailedCount() { failedCount = 0; }
    }
}