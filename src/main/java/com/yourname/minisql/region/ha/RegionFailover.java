package com.yourname.minisql.region.ha;

import com.yourname.minisql.region.zk.ZkConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Region 故障检测与自动切换
 */
public class RegionFailover {
    private static final Logger log = LoggerFactory.getLogger(RegionFailover.class);
    
    private final CuratorFramework zkClient;
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
    
    public RegionFailover(CuratorFramework zkClient, FailoverListener listener) {
        this.zkClient = zkClient;
        this.failoverListener = listener;
        this.healthChecker = Executors.newSingleThreadScheduledExecutor();
    }
    
    public void start() throws Exception {
        // 监听 Region 节点变化
        watchRegionNodes();
        
        // 定期健康检查
        healthChecker.scheduleAtFixedRate(this::healthCheck, 
            HEALTH_CHECK_INTERVAL_SEC, HEALTH_CHECK_INTERVAL_SEC, TimeUnit.SECONDS);
        
        log.info("Region failover started");
    }
    
    public void stop() {
        healthChecker.shutdown();
        log.info("Region failover stopped");
    }
    
    private void watchRegionNodes() throws Exception {
        String path = ZkConfig.ZK_REGIONS_PATH;
        CuratorCache cache = CuratorCache.build(zkClient, path);
        cache.listenable().addListener(CuratorCacheListener.builder()
            .forDeletes(node -> {
                String nodePath = node.getPath();
                String regionId = extractRegionId(nodePath);
                if (regionId != null) {
                    RegionHealth health = regionHealthMap.remove(regionId);
                    if (health != null && health.isHealthy()) {
                        log.warn("Region node deleted: {}", regionId);
                        if (failoverListener != null) {
                            failoverListener.onRegionFailed(regionId, health.getAddress());
                        }
                    }
                }
            })
            .build()
        );
        cache.start();
    }
    
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
    
    public void registerRegion(String regionId, String address) {
        regionHealthMap.put(regionId, new RegionHealth(regionId, address));
        log.info("Registered region: {} -> {}", regionId, address);
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