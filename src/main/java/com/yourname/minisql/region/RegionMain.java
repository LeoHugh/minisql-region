package com.yourname.minisql.region;

import com.yourname.minisql.region.manager.DatabaseManager;
import com.yourname.minisql.region.network.RegionServer;
import com.yourname.minisql.region.zk.RegionRegistry;
import com.yourname.minisql.region.zk.ZkClientManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

public class RegionMain {
    private static final Logger log = LoggerFactory.getLogger(RegionMain.class);
    
    private static RegionServer regionServer;
    private static DatabaseManager dbManager;
    private static RegionRegistry regionRegistry;
    
    public static void main(String[] args) throws Exception {
        int port = 8888;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        
        String host = InetAddress.getLocalHost().getHostAddress();
        if (args.length > 1) {
            host = args[1];
        }
        
        System.out.println("Starting Region on " + host + ":" + port);
        String dataDir = "./data_region_" + port;
        
        // 初始化数据库引擎
        dbManager = new DatabaseManager(dataDir);
        
        // 初始化 ZK 注册
        try {
            ZkClientManager.getInstance().init();
            regionRegistry = new RegionRegistry(host, port);
            regionRegistry.register();
            System.out.println("Registered to Zookeeper at " + host + ":" + port);
        } catch (Exception e) {
            System.err.println("Failed to register to Zookeeper: " + e.getMessage());
            e.printStackTrace();
            // 即使注册失败，Region 也继续运行（降级模式）
        }
        
        // 启动网络服务
        regionServer = new RegionServer(port, dbManager);
        
        // 添加 shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down Region...");
            try {
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
}