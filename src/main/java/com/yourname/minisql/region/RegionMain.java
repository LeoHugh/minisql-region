package com.yourname.minisql.region;

import com.yourname.minisql.region.manager.DatabaseManager;
import com.yourname.minisql.region.network.RegionServer;

public class RegionMain {
    public static void main(String[] args) throws Exception {
        int port = 8888;  // 第一个 Region 用 8888，第二个用 8889
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        
        System.out.println("Starting Region on port: " + port);
        String dataDir = "./data_region_" + port;
        
        DatabaseManager dbManager = new DatabaseManager(dataDir);
        RegionServer server = new RegionServer(port, dbManager);
        
        // 添加 shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.out.println("Shutting down Region...");
                server.stop();
                dbManager.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
        
        server.start();
    }
}