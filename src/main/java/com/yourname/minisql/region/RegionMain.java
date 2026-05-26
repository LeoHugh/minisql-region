package com.yourname.minisql.region;

import com.yourname.minisql.region.manager.DatabaseManager;
import com.yourname.minisql.region.network.RegionServer;
import com.yourname.minisql.region.replication.ReplicationManager;
import com.yourname.minisql.region.zk.RegionRegistry;
import com.yourname.minisql.region.zk.ZkClientManager;
import com.yourname.minisql.region.zk.ZkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



// Region节点启动入口
// 用法: RegionMain <port> <host> <role> <groupId>
//   role: master / slave / standby（默认）
//   groupId: 所属分组（Replica Group / Shard），同一组内一个 Master 多个 Slave
//   Slave 节点将通过 ZK 自动发现同组 Master 的复制地址
public class RegionMain {
    private static final Logger log = LoggerFactory.getLogger(RegionMain.class);
    
    private static RegionServer regionServer;
    private static DatabaseManager dbManager;
    private static RegionRegistry regionRegistry;
    private static ReplicationManager replicationManager;
    
    public static void main(String[] args) throws Exception {
        // 参数校验
        if (args.length < 4) {
            System.err.println("Usage: RegionMain <port> <host> <role> <groupId>");
            System.err.println("  port    : Region 服务端口");
            System.err.println("  host    : 绑定地址（如 localhost 或 192.168.1.100）");
            System.err.println("  role    : master / slave / standby");
            System.err.println("  groupId : 所属分组 ID（如 group1, shard-0）");
            System.exit(1);
        }
        
        int port = Integer.parseInt(args[0]);
        String host = args[1];
        String role = args[2].toLowerCase();
        String groupId = args[3];
        
        System.out.println("Starting Region on " + host + ":" + port 
                         + " [role=" + role.toUpperCase() + ", group=" + groupId + "]");
        String dataDir = "./data_region_" + port;
        
        // 初始化数据库引擎
        dbManager = new DatabaseManager(dataDir);
        
        // 初始化复制管理器
        String regionId = host + ":" + port;
        replicationManager = new ReplicationManager(dbManager, regionId);
        dbManager.setReplicationManager(replicationManager);
        log.info("ReplicationManager created for region: {}", regionId);
        
        // 初始化 ZK 注册（按 groupId 分组注册）
        try {
            ZkClientManager.getInstance().init();
            regionRegistry = new RegionRegistry(host, port, groupId);
            regionRegistry.register();
            System.out.println("Registered to Zookeeper at " + host + ":" + port + " [group=" + groupId + "]");
        } catch (Exception e) {
            System.err.println("Failed to register to Zookeeper: " + e.getMessage());
            e.printStackTrace();
            // 即使注册失败，Region 也继续运行（待实现）
        }
        
        // 根据角色激活复制功能
        activateReplicationRole(role, host, port, groupId);
        
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
     * Slave 通过 ZK 自动发现同组 Master
     */
    private static void activateReplicationRole(String role, String host, int port, String groupId) {
        switch (role) {
            case "master":
                replicationManager.becomeMaster();
                int replicationPort = replicationManager.getReplicationPort();
                System.out.println("=== MASTER mode activated [group=" + groupId 
                                 + "], replication port: " + replicationPort + " ===");
                
                // 将复制角色和端口信息更新到 ZK
                if (regionRegistry != null) {
                    regionRegistry.updateReplicationInfo("MASTER", replicationPort);
                } else {
                    log.warn("RegionRegistry is null, cannot update ZK replication info");
                }
                break;
                
            case "slave":
                // 从 ZK 自动发现同组的 Master 复制地址
                String masterReplicationAddress = null;
                if (regionRegistry != null) {
                    masterReplicationAddress = regionRegistry.discoverMasterReplicationAddress();
                } else {
                    log.warn("RegionRegistry is null, cannot discover master replication address");
                }
                
                if (masterReplicationAddress != null) {
                    replicationManager.becomeSlave(masterReplicationAddress);
                    System.out.println("=== SLAVE mode activated [group=" + groupId 
                                     + "], master replication address: " + masterReplicationAddress + " ===");
                    
                    // 更新 ZK 中的角色信息
                    if (regionRegistry != null) {
                        regionRegistry.updateReplicationInfo("SLAVE", 0);
                    }
                } else {
                    System.err.println("WARNING: Slave mode requested but no master found in group '" + groupId + "'!");
                    System.err.println("  Ensure a MASTER region with the same groupId is registered in ZooKeeper.");
                    System.err.println("  Falling back to STANDBY mode.");
                    if (regionRegistry != null) {
                        regionRegistry.updateReplicationInfo("STANDBY", 0);
                    }
                }
                break;
                
            default: // standby
                System.out.println("=== STANDBY mode (no replication) [group=" + groupId + "] ===");
                if (regionRegistry != null) {
                    regionRegistry.updateReplicationInfo("STANDBY", 0);
                }
                break;
        }
    }
}