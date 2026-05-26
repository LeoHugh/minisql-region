package com.yourname.minisql.region;

import com.yourname.minisql.region.manager.DatabaseManager;
import com.yourname.minisql.region.network.RegionServer;
import com.yourname.minisql.region.replication.ReplicationManager;
import com.yourname.minisql.region.zk.RegionRegistry;
import com.yourname.minisql.region.zk.ZkClientManager;
import com.yourname.minisql.region.zk.ZkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

// Region节点启动入口
// 用法: RegionMain <port> [host] [role] [masterReplicationAddress]
//   role: master / slave / standby（默认）
//   masterReplicationAddress: 仅 slave 模式需要，格式 host:port（Master的复制端口）
//                             如果不指定，将通过 ZK 自动发现
public class RegionMain {
    private static final Logger log = LoggerFactory.getLogger(RegionMain.class);
    
    private static RegionServer regionServer;
    private static DatabaseManager dbManager;
    private static RegionRegistry regionRegistry;
    private static ReplicationManager replicationManager;
    
    public static void main(String[] args) throws Exception {
        //有默认配置，也可以通过命令行读取
        int port = 8888;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        
        String host = InetAddress.getLocalHost().getHostAddress();
        if (args.length > 1) {
            host = args[1];
        }
        
        // 解析复制角色: master / slave / standby（默认）
        String role = "standby";
        if (args.length > 2) {
            role = args[2].toLowerCase();
        }
        
        // 解析 Master 复制地址（仅 slave 模式时使用）
        String masterReplicationAddress = null;
        if (args.length > 3) {
            masterReplicationAddress = args[3];
        }
        
        System.out.println("Starting Region on " + host + ":" + port + " [role=" + role.toUpperCase() + "]");
        String dataDir = "./data_region_" + port;
        
        // 初始化数据库引擎
        dbManager = new DatabaseManager(dataDir);
        
        // 初始化复制管理器
        String regionId = host + ":" + port;
        replicationManager = new ReplicationManager(dbManager, regionId);
        dbManager.setReplicationManager(replicationManager);
        log.info("ReplicationManager created for region: {}", regionId);
        
        // 初始化 ZK 注册
        try {
            ZkClientManager.getInstance().init();
            regionRegistry = new RegionRegistry(host, port);
            regionRegistry.register();
            System.out.println("Registered to Zookeeper at " + host + ":" + port);
        } catch (Exception e) {
            System.err.println("Failed to register to Zookeeper: " + e.getMessage());
            e.printStackTrace();
            // 即使注册失败，Region 也继续运行（待实现）
        }
        
        // 根据角色激活复制功能
        activateReplicationRole(role, host, port, masterReplicationAddress);
        
        // 启动网络服务（传入 replicationManager 以支持 Slave 只读保护）
        regionServer = new RegionServer(port, dbManager, replicationManager);
        
        // 添加 shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down Region...");
            try {
                if (replicationManager != null) {
                    replicationManager.close();
                    System.out.println("ReplicationManager closed");
                }
                if (regionRegistry != null) {
                    regionRegistry.unregister();
                }
                if (regionServer != null) {
                    regionServer.stop();
                }
                if (dbManager != null) {
                    dbManager.close();
                }
                ZkClientManager.getInstance().close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
        
        regionServer.start();
    }
    
    /**
     * 根据角色激活复制功能
     */
    private static void activateReplicationRole(String role, String host, int port, 
                                                  String masterReplicationAddress) {
        switch (role) {
            case "master":
                replicationManager.becomeMaster();
                int replicationPort = replicationManager.getReplicationPort();
                System.out.println("=== MASTER mode activated, replication port: " + replicationPort + " ===");
                
                // 将复制角色和端口信息更新到 ZK
                if (regionRegistry != null) {
                    regionRegistry.updateReplicationInfo("MASTER", replicationPort);
                } else {
                    log.warn("RegionRegistry is null, cannot update ZK replication info");
                }
                break;
                
            case "slave":
                // 如果未指定 Master 地址，尝试从 ZK 自动发现
                if (masterReplicationAddress == null || masterReplicationAddress.isEmpty()) {
                    if (regionRegistry != null) {
                        masterReplicationAddress = regionRegistry.discoverMasterReplicationAddress();
                    } else {
                        log.warn("RegionRegistry is null, cannot discover master replication address");
                    }
                }
                
                if (masterReplicationAddress != null) {
                    replicationManager.becomeSlave(masterReplicationAddress);
                    System.out.println("=== SLAVE mode activated, master replication address: " 
                                       + masterReplicationAddress + " ===");
                    
                    // 更新 ZK 中的角色信息
                    if (regionRegistry != null) {
                        regionRegistry.updateReplicationInfo("SLAVE", 0);
                    }
                } else {
                    System.err.println("WARNING: Slave mode requested but no master replication address found!");
                    System.err.println("  Please specify via command line: RegionMain <port> <host> slave <masterHost:masterReplicationPort>");
                    System.err.println("  Or ensure a MASTER region is registered in ZooKeeper.");
                    System.err.println("  Falling back to STANDBY mode.");
                    if (regionRegistry != null) {
                        regionRegistry.updateReplicationInfo("STANDBY", 0);
                    }
                }
                break;
                
            default: // standby
                System.out.println("=== STANDBY mode (no replication) ===");
                if (regionRegistry != null) {
                    regionRegistry.updateReplicationInfo("STANDBY", 0);
                }
                break;
        }
    }
}