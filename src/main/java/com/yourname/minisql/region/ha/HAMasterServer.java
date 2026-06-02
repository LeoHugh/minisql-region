package com.yourname.minisql.region.ha;


import com.yourname.minisql.region.loadbalance.LoadBalancer;
import com.yourname.minisql.region.ha.masterengine.MasterServer;
import com.yourname.minisql.region.ha.masterengine.MetadataManager;
import com.yourname.minisql.region.ha.masterengine.TopologyManager;
import com.yourname.minisql.region.zk.ZkClientManager;
import com.yourname.minisql.region.zk.ZkConfig;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 高可用 Master 服务器（编排层）
 *
 * 负责编排三个 Manager 和 MasterServer 的生命周期：
 *   1. MetadataManager  : table → group 映射 + ZK 持久化
 *   2. TopologyManager  : 集群拓扑 + 故障检测
 *   3. LoadBalancer      : 无状态策略选择器
 *   4. MasterServer      : 网络 Facade
 *
 * 通过 MasterElection 实现 Leader 选举：
 *   - 成为 Leader 时启动上述组件
 *   - 失去 Leadership 时停止上述组件
 */
public class HAMasterServer {
    private static final Logger log = LoggerFactory.getLogger(HAMasterServer.class);
    
    private final int port;
    private final String masterId;
    
    // 网络层
    private MasterServer masterServer;
    private MasterElection election;
    private Thread masterThread;
    
    // 三个核心 Manager
    private MetadataManager metadataManager;
    private TopologyManager topologyManager;
    private LoadBalancer loadBalancer;
    
    private volatile boolean running = true;
    
    public HAMasterServer(int port, String masterId) {
        this.port = port;
        this.masterId = masterId;
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
                startMasterEngine();
            }
            
            @Override
            public void onBecomeFollower() {
                log.info("=== This master ({}) became FOLLOWER ===", masterId);
                stopMasterEngine();
            }
            
            @Override
            public void onMasterFailed() {
                log.error("Master failed! Stopping MasterServer...");
                stopMasterEngine();
            }
        });
        
        // 启动选举
        election.start();
        
        log.info("HA Master server started on port {}, id: {}", port, masterId);
    }
    
    private void startMasterEngine() {
        if (masterServer != null) {
            log.warn("MasterServer already running");
            return;
        }
        
        try {
            // 1. 创建三个 Manager
            metadataManager = new MetadataManager();
            topologyManager = new TopologyManager();
            loadBalancer = new LoadBalancer();
            
            // 2. 启动 MetadataManager（从 ZK 加载表映射）
            metadataManager.start();
            log.info("MetadataManager started");
            
            // 3. 启动 TopologyManager（初始化 ServiceDiscovery + RegionFailover）
            topologyManager.start();
            log.info("TopologyManager started");
            
            // 4. 构造 MasterServer（网络 Facade），注入三个 Manager
            masterServer = new MasterServer(port, metadataManager, topologyManager, loadBalancer);
            
            // 5. 写入 Active Master 地址到 ZK
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
            
            // 6. 在独立线程中启动 MasterServer（Socket accept 会阻塞）
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
    
    private void stopMasterEngine() {
        // 1. 停止 MasterServer（网络层）
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
        
        // 2. 停止 TopologyManager（包含 ServiceDiscovery + RegionFailover）
        if (topologyManager != null) {
            try {
                topologyManager.stop();
                log.info("TopologyManager stopped");
            } catch (Exception e) {
                log.error("Error stopping TopologyManager", e);
            }
            topologyManager = null;
        }
        
        // 3. 停止 MetadataManager
        if (metadataManager != null) {
            try {
                metadataManager.stop();
                log.info("MetadataManager stopped");
            } catch (Exception e) {
                log.error("Error stopping MetadataManager", e);
            }
            metadataManager = null;
        }
        
        loadBalancer = null;
        
        // 4. 等待 Master 线程结束
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
        
        stopMasterEngine();
        
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