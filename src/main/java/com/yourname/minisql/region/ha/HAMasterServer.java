package com.yourname.minisql.region.ha;

import com.yourname.minisql.region.loadbalance.LoadBalancer;
import com.yourname.minisql.region.master.MasterServer;
import com.yourname.minisql.region.zk.ZkClientManager;
import com.yourname.minisql.region.zk.ZkConfig;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 高可用 Master 服务器
 */
public class HAMasterServer {
    private static final Logger log = LoggerFactory.getLogger(HAMasterServer.class);
    
    private final int port;
    private final String masterId;
    private MasterServer masterServer;
    private MasterElection election;
    private LoadBalancer loadBalancer;
    private RegionFailover regionFailover;
    private volatile boolean running = true;
    private Thread masterThread;
    
    public HAMasterServer(int port, String masterId) {
        this.port = port;
        this.masterId = masterId;
        this.loadBalancer = new LoadBalancer();
    }
    
    public void start() throws Exception {
        // 确保 ZK 客户端已初始化
        ZkClientManager.getInstance().init();
        CuratorFramework zkClient = ZkClientManager.getInstance().getClient();
        
        // 确保 Master 相关路径存在
        ZkClientManager.getInstance().ensurePathExists(ZkConfig.ZK_MASTER_PATH);
        
        // 初始化 Master 选举
        election = new MasterElection(zkClient, ZkConfig.ZK_MASTER_PATH + "/election", masterId);
        
        // 设置状态监听
        election.setStateListener(new MasterElection.MasterStateListener() {
            @Override
            public void onBecomeLeader() {
                log.info("=== This master ({}) became LEADER ===", masterId);
                startMasterServer();
            }
            
            @Override
            public void onBecomeFollower() {
                log.info("=== This master ({}) became FOLLOWER ===", masterId);
                stopMasterServer();
            }
            
            @Override
            public void onMasterFailed() {
                log.error("Master failed! Stopping MasterServer...");
                stopMasterServer();
            }
        });
        
        // 启动选举
        election.start();
        
        log.info("HA Master server started on port {}, id: {}", port, masterId);
    }
    
    private void startMasterServer() {
        if (masterServer != null) {
            log.warn("MasterServer already running");
            return;
        }
        
        try {
            masterServer = new MasterServer(port);
            
            // 写入 Active Master 地址到 ZK
            String activeMasterAddr = getLocalHostAddress() + ":" + port;
            try {
                String activeMasterPath = ZkConfig.ZK_MASTER_PATH + "/active";
                // 先删除旧的节点（如果存在）
                try {
                    ZkClientManager.getInstance().deleteNode(activeMasterPath);
                } catch (Exception e) {
                    // 节点不存在，忽略
                }
                ZkClientManager.getInstance().createEphemeralNode(activeMasterPath, activeMasterAddr.getBytes());
                log.info("Registered Active Master in ZK: {}", activeMasterAddr);
            } catch (Exception e) {
                log.error("Failed to register active master in ZK", e);
            }
            
            // 启动 Region 故障检测
            try {
                CuratorFramework zkClient = ZkClientManager.getInstance().getClient();
                regionFailover = new RegionFailover(zkClient, new RegionFailover.FailoverListener() {
                    @Override
                    public void onRegionFailed(String regionId, String address) {
                        log.warn("Region failed: {} ({})", regionId, address);
                        if (loadBalancer != null) {
                            loadBalancer.markAvailable(address, false);
                        }
                    }
                    
                    @Override
                    public void onRegionRecovered(String regionId, String address) {
                        log.info("Region recovered: {} ({})", regionId, address);
                        if (loadBalancer != null) {
                            loadBalancer.markAvailable(address, true);
                        }
                    }
                    
                    @Override
                    public void onFailoverCompleted(String fromRegion, String toRegion) {
                        log.info("Failover completed: {} -> {}", fromRegion, toRegion);
                    }
                });
                regionFailover.start();
                log.info("RegionFailover started");
            } catch (Exception e) {
                log.error("Failed to start region failover", e);
            }
            
            // 在独立线程中启动 MasterServer
            masterThread = new Thread(() -> {
                try {
                    log.info("MasterServer starting on port {}", port);
                    masterServer.start();
                } catch (Exception e) {
                    log.error("MasterServer stopped unexpectedly", e);
                } finally {
                    log.info("MasterServer on port {} has stopped", port);
                }
            }, "MasterServer-" + port);
            masterThread.start();
            
            log.info("MasterServer started on port {}", port);
            
        } catch (Exception e) {
            log.error("Failed to start MasterServer", e);
        }
    }
    
    private void stopMasterServer() {
        // 停止 RegionFailover
        if (regionFailover != null) {
            try {
                regionFailover.stop();
                regionFailover = null;
                log.info("RegionFailover stopped");
            } catch (Exception e) {
                log.error("Error stopping RegionFailover", e);
            }
        }
        
        // 停止 MasterServer
        if (masterServer != null) {
            try {
                // 从 ZK 删除 Active Master 节点
                try {
                    String activeMasterPath = ZkConfig.ZK_MASTER_PATH + "/active";
                    ZkClientManager.getInstance().deleteNode(activeMasterPath);
                    log.info("Removed Active Master node from ZK");
                } catch (Exception e) {
                    log.debug("Could not delete active master node: {}", e.getMessage());
                }
                
                masterServer.stop();
                log.info("MasterServer stopped");
            } catch (Exception e) {
                log.error("Failed to stop MasterServer", e);
            } finally {
                masterServer = null;
            }
        }
        
        // 等待 Master 线程结束
        if (masterThread != null && masterThread.isAlive()) {
            try {
                masterThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for Master thread");
            }
            masterThread = null;
        }
    }
    
    public void stop() {
        if (!running) {
            log.debug("HAMasterServer already stopped");
            return;
        }
        running = false;
        
        log.info("Stopping HAMasterServer on port {}, id: {}", port, masterId);
        
        stopMasterServer();
        
        if (election != null) {
            try {
                election.stop();
                log.info("MasterElection stopped");
            } catch (IllegalStateException e) {
                log.debug("MasterElection already closed: {}", e.getMessage());
            } catch (Exception e) {
                log.error("Error stopping MasterElection", e);
            }
            election = null;
        }
        
        log.info("HAMasterServer stopped");
    }
    
    public LoadBalancer getLoadBalancer() {
        return loadBalancer;
    }
    
    private String getLocalHostAddress() {
        try {
            return java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (java.net.UnknownHostException e) {
            return "localhost";
        }
    }
    
    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 9999;
        String masterId = args.length > 1 ? args[1] : "master-1";
        
        HAMasterServer haMaster = new HAMasterServer(port, masterId);
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down HA Master...");
            haMaster.stop();
        }));
        
        haMaster.start();
        
        // 保持主线程运行
        Thread.currentThread().join();
    }
}