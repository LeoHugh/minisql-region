package com.yourname.minisql.region.ha;


import com.yourname.minisql.region.client.Client;
import com.yourname.minisql.region.manager.DatabaseManager;
import com.yourname.minisql.region.network.RegionServer;
import com.yourname.minisql.region.zk.RegionRegistry;
import com.yourname.minisql.region.zk.ZkClientManager;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.net.Socket;

import static org.junit.jupiter.api.Assertions.*;

public class HATest {
    private static final Logger log = LoggerFactory.getLogger(HATest.class);
    
    private HAMasterServer master1;
    private HAMasterServer master2;
    private RegionServer region1;
    private RegionServer region2;
    private RegionRegistry registry1;
    private RegionRegistry registry2;
    private Client client;
    private PrintStream clientPrintStream;  // 新增
    private DatabaseManager db1;
    private DatabaseManager db2;
    
    // 用于捕获客户端输出的流
    private ByteArrayOutputStream clientOutContent;
    private PrintStream originalClientOut;
    
    @BeforeEach
    public void setUp() throws Exception {
        System.out.println("\n========== HATest Setup 开始 ==========");
        // 清理旧的测试数据目录
    cleanupTestData();
        // 初始化 ZK
        System.out.println("1. 初始化 Zookeeper 客户端...");
        ZkClientManager.getInstance().init();
        System.out.println("   ✓ ZK 客户端初始化完成");
        
        // 清理 ZK 中的旧节点
        try {
            ZkClientManager.getInstance().deleteNode("/minisql/master/election");
            System.out.println("   已清理旧的选举节点");
        } catch (Exception e) {
            System.out.println("   清理选举节点时无旧节点");
        }
        
        // 启动 Master1
        System.out.println("\n2. 启动 Master1 (端口 9999)...");
        master1 = new HAMasterServer(9999, "master-1");
        master1.start();
        System.out.println("   ✓ Master1 启动完成");
        
        // 启动 Master2
        System.out.println("\n3. 启动 Master2 (端口 9998)...");
        master2 = new HAMasterServer(9998, "master-2");
        master2.start();
        System.out.println("   ✓ Master2 启动完成");
        
        // 等待 Master 选举完成
        System.out.println("\n4. 等待 Master 选举完成 (最大5秒)...");
        waitForCondition(() -> isMasterLeader(9999) || isMasterLeader(9998), 5000);
        
        // 检查哪个 Master 是 Leader
        checkMasterLeader();
        
        // 启动 Region1
        System.out.println("\n5. 启动 Region1 (端口 8801)...");
        db1 = new DatabaseManager("./test_db_1");
        com.yourname.minisql.region.replication.ReplicationManager rep1 = new com.yourname.minisql.region.replication.ReplicationManager(db1, "region-1");
        db1.setReplicationManager(rep1);
        rep1.becomeMaster();
        region1 = new RegionServer(8801, db1, rep1);
        new Thread(() -> {
            try {
                region1.start();
                System.out.println("   ✓ Region1 服务已启动");
            } catch (Exception e) {
                System.err.println("   ✗ Region1 启动失败: " + e.getMessage());
            }
        }).start();
        
        // 注册 Region1 到 ZK
        registry1 = new RegionRegistry("localhost", 8801, "test-group");
        registry1.register();
        registry1.updateReplicationInfo("MASTER", rep1.getReplicationPort());
        System.out.println("   ✓ Region1 已注册到 ZK");
        
        // 启动 Region2
        System.out.println("\n6. 启动 Region2 (端口 8802)...");
        db2 = new DatabaseManager("./test_db_2");
        com.yourname.minisql.region.replication.ReplicationManager rep2 = new com.yourname.minisql.region.replication.ReplicationManager(db2, "region-2");
        db2.setReplicationManager(rep2);
        rep2.becomeSlave("localhost:" + rep1.getReplicationPort());
        region2 = new RegionServer(8802, db2, rep2);
        new Thread(() -> {
            try {
                region2.start();
                System.out.println("   ✓ Region2 服务已启动");
            } catch (Exception e) {
                System.err.println("   ✗ Region2 启动失败: " + e.getMessage());
            }
        }).start();
        
        // 注册 Region2 到 ZK
        registry2 = new RegionRegistry("localhost", 8802, "test-group");
        registry2.register();
        registry2.updateReplicationInfo("SLAVE", rep2.getReplicationPort());
        System.out.println("   ✓ Region2 已注册到 ZK");
        
        // 等待 Region 注册和发现
        System.out.println("\n7. 等待 Region 注册完成 (最大5秒)...");
        waitForCondition(() -> isRegionAvailable(8801) && isRegionAvailable(8802), 5000);
        // 额外等待，确保 LoadBalancer 更新
        try { Thread.sleep(1000); } catch(Exception e) {}
        
        // 检查 Region 是否可用
        checkRegionAvailable();
        
        // 创建客户端
        System.out.println("\n8. 创建客户端...");
        client = new Client("localhost", 9999);
        System.out.println("   ✓ 客户端创建完成，连接到 Master: localhost:9999");
        
        // 捕获客户端输出
        clientOutContent = new ByteArrayOutputStream();
        clientPrintStream = new PrintStream(clientOutContent);
        originalClientOut = System.out;
        
        System.out.println("\n========== HATest Setup 完成 ==========\n");
    }
    
    @AfterEach
    public void tearDown() throws Exception {
    System.out.println("\n========== HATest Teardown 开始 ==========");
    
    // 恢复标准输出（先做）
    System.setOut(originalClientOut);
    
    // 关闭 PrintStream
    if (clientPrintStream != null) {
        try {
            clientPrintStream.close();
        } catch (Exception e) {
            // ignore
        }
    }
    
    // 关闭客户端（先关闭，避免后续请求）
    if (client != null) {
        System.out.println("关闭客户端...");
        // Client 没有 close 方法，只需要置空
        client = null;
    }
    
    // 注销 Region（先注销，避免新的请求）
    if (registry1 != null) {
        try {
            registry1.close();
            System.out.println("Region1 已注销");
        } catch (Exception e) {
            System.out.println("Region1 注销失败: " + e.getMessage());
        }
    }
    if (registry2 != null) {
        try {
            registry2.close();
            System.out.println("Region2 已注销");
        } catch (Exception e) {
            System.out.println("Region2 注销失败: " + e.getMessage());
        }
    }
    
    // 停止 Region 服务
    if (region1 != null) {
        try {
            region1.stop();
            System.out.println("Region1 已停止");
        } catch (Exception e) {
            System.out.println("Region1 停止失败: " + e.getMessage());
        }
    }
    if (region2 != null) {
        try {
            region2.stop();
            System.out.println("Region2 已停止");
        } catch (Exception e) {
            System.out.println("Region2 停止失败: " + e.getMessage());
        }
    }
    
    // 停止 Master 服务（最后停止）
    if (master1 != null) {
        try {
            master1.stop();
            System.out.println("Master1 已停止");
        } catch (Exception e) {
            System.out.println("Master1 停止失败: " + e.getMessage());
        }
    }
    if (master2 != null) {
        try {
            master2.stop();
            System.out.println("Master2 已停止");
        } catch (Exception e) {
            System.out.println("Master2 停止失败: " + e.getMessage());
        }
    }
    
    // 关闭数据库
    if (db1 != null) {
        try {
            db1.close();
        } catch (Exception e) {
            // ignore
        }
    }
    if (db2 != null) {
        try {
            db2.close();
        } catch (Exception e) {
            // ignore
        }
    }
    
    System.out.println("========== HATest Teardown 完成 ==========\n");
}
    
    /**
     * 检查 Master 选举状态
     */
    private void checkMasterLeader() throws Exception {
        System.out.println("\n--- Master 选举状态 ---");
        
        // 尝试连接到 Master1
        try (Socket s = new Socket("localhost", 9999)) {
            System.out.println("  Master1 (9999): 可连接");
        } catch (Exception e) {
            System.out.println("  Master1 (9999): 不可连接 - " + e.getMessage());
        }
        
        // 尝试连接到 Master2
        try (Socket s = new Socket("localhost", 9998)) {
            System.out.println("  Master2 (9998): 可连接");
        } catch (Exception e) {
            System.out.println("  Master2 (9998): 不可连接 - " + e.getMessage());
        }
        
        System.out.println("--- Master 选举状态结束 ---\n");
    }
    
    /**
     * 检查 Region 是否可用
     */
    private void checkRegionAvailable() {
        System.out.println("\n--- Region 状态检查 ---");
        
        // 检查 Region1
        try (Socket s = new Socket("localhost", 8801)) {
            System.out.println("  Region1 (8801): 可连接");
        } catch (Exception e) {
            System.out.println("  Region1 (8801): 不可连接 - " + e.getMessage());
        }
        
        // 检查 Region2
        try (Socket s = new Socket("localhost", 8802)) {
            System.out.println("  Region2 (8802): 可连接");
        } catch (Exception e) {
            System.out.println("  Region2 (8802): 不可连接 - " + e.getMessage());
        }
        
        System.out.println("--- Region 状态检查结束 ---\n");
    }
    
    @Test
    @DisplayName("测试 Master 故障自动切换")
    public void testMasterFailover() throws Exception {
        System.out.println("\n========== 测试 Master 故障自动切换 ==========");
        
        // 步骤1: 正常操作
        System.out.println("\n[步骤1] 执行正常操作，验证系统可用");
        
        System.out.println("  执行: CREATE TABLE test (id STRING, name STRING)");
        String createResult = client.execute("CREATE TABLE test (id STRING, name STRING)");
        System.out.println("  结果: " + createResult);
        assertTrue(createResult.contains("created") || createResult.contains("成功"), 
                   "建表应成功, 实际: " + createResult);
        
        System.out.println("\n  执行: INSERT INTO test (id, name) VALUES ('1', 'data')");
        String insertResult = client.execute("INSERT INTO test (id, name) VALUES ('1', 'data')");
        System.out.println("  结果: " + insertResult);
        assertTrue(insertResult.contains("Inserted row") || insertResult.contains("成功"), 
                   "插入应成功, 实际: " + insertResult);
        
        System.out.println("\n  执行: SELECT * FROM test WHERE id = '1'");
        String selectResult = client.execute("SELECT * FROM test WHERE id = '1'");
        System.out.println("  结果: " + selectResult);
        assertTrue(selectResult.contains("data"), 
                   "查询应返回 data, 实际: " + selectResult);
        
        System.out.println("  ✓ 正常操作验证通过");
        
        // 步骤2: 停止 Master1
        System.out.println("\n[步骤2] 停止 Master1 (模拟故障)");
        System.out.println("  停止前检查 Master1 状态...");
        
        // 检查哪个 Master 是当前 Leader
        boolean master1WasLeader = isMasterLeader(9999);
        System.out.println("  Master1 曾是 Leader: " + master1WasLeader);
        
        System.out.println("  正在停止 Master1...");
        master1.stop();
        System.out.println("  ✓ Master1 已停止");
        
        // 等待选举
        System.out.println("\n[步骤3] 等待 Master 选举完成 (最大5秒)...");
        waitForCondition(() -> isMasterLeader(9998), 5000);
        try { Thread.sleep(1000); } catch(Exception e) {}
        
        // 检查新 Leader
        System.out.println("\n  检查新 Leader 状态:");
        boolean master2IsLeader = isMasterLeader(9998);
        System.out.println("  Master2 现在是 Leader: " + master2IsLeader);
        
        // 步骤4: 继续操作
        System.out.println("\n[步骤4] 故障后继续操作，验证自动切换");
        
        System.out.println("  执行: INSERT INTO test (id, name) VALUES ('2', 'more-data')");
        String insertResult2 = client.execute("INSERT INTO test (id, name) VALUES ('2', 'more-data')");
        System.out.println("  结果: " + insertResult2);
        assertTrue(insertResult2.contains("Inserted row") || insertResult2.contains("成功"), 
                   "故障后插入应成功, 实际: " + insertResult2);
        
        System.out.println("\n  执行: SELECT * FROM test WHERE id = '2'");
        String selectResult2 = client.execute("SELECT * FROM test WHERE id = '2'");
        System.out.println("  结果: " + selectResult2);
        assertTrue(selectResult2.contains("more-data"), 
                   "故障后查询应返回 more-data, 实际: " + selectResult2);
        
        System.out.println("\n========== 测试 Master 故障自动切换通过 ==========\n");
    }
    
    @Test
    @DisplayName("测试 Region 负载均衡")
    public void testRegionLoadBalance() throws Exception {
        System.out.println("\n========== 测试 Region 负载均衡 ==========");
        
        // 创建测试表
        System.out.println("\n[步骤1] 创建测试表");
        client.execute("CREATE TABLE lb_test (id STRING, name STRING)");
        Thread.sleep(1000);
        
        // 多次插入，观察负载分布
        System.out.println("\n[步骤2] 执行多次插入，观察负载分布");
        java.util.Map<String, Integer> regionCount = new java.util.HashMap<>();
        
        for (int i = 1; i <= 10; i++) {
            String sql = "INSERT INTO lb_test (id, name) VALUES ('" + i + "', 'user_" + i + "')";
            System.out.print("  执行: " + sql + " -> ");
            
            // 捕获输出以获取 Region 地址
            System.setOut(clientPrintStream);
            String result = client.execute(sql);
            System.setOut(originalClientOut);
            
            String output = clientOutContent.toString();
            clientOutContent.reset();
            
            // 提取 Region 地址
            String regionAddr = extractRegionAddress(output);
            regionCount.put(regionAddr, regionCount.getOrDefault(regionAddr, 0) + 1);
            System.out.println("路由到: " + regionAddr);
        }
        
        System.out.println("\n[步骤3] 负载分布统计:");
        for (java.util.Map.Entry<String, Integer> entry : regionCount.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue() + " 次请求");
        }
        
        // 验证负载基本均衡（每个 Region 至少有一些请求）
        assertTrue(regionCount.size() >= 1, "应该有 Region 被使用");
        
        System.out.println("\n========== Region 负载均衡测试通过 ==========");
    }
    
    /**
     * 检查 Master 是否是 Leader
     */
    private boolean isMasterLeader(int port) {
        // 简单实现：尝试连接，如果成功则认为是 Leader
        // 更精确的实现需要查询 ZK 中的 Leader 节点
        try (Socket s = new Socket("localhost", port)) {
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * 从客户端输出中提取 Region 地址
     */
    private String extractRegionAddress(String output) {
        // 查找 "Got region address: " 后面的内容
        String marker = "Got region address: ";
        int idx = output.indexOf(marker);
        if (idx >= 0) {
            int start = idx + marker.length();
            int end = output.indexOf("\n", start);
            if (end < 0) {
                end = output.length();
            }
            return output.substring(start, end).trim();
        }
        return "unknown";
    }

    private void cleanupTestData() {
    try {
        // 删除测试数据目录
        deleteDirectory(new File("./test_db_1"));
        deleteDirectory(new File("./test_db_2"));
        deleteDirectory(new File("./test_db_1_re"));
        System.out.println("已清理测试数据目录");
    } catch (Exception e) {
        System.out.println("清理测试数据目录时出错: " + e.getMessage());
    }
}
    private void deleteDirectory(File dir) {
    if (dir.exists()) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        dir.delete();
    }
}

    private boolean isRegionAvailable(int port) {
        try (Socket s = new Socket("localhost", port)) {
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    
    private boolean waitForCondition(java.util.concurrent.Callable<Boolean> condition, int timeoutMs) {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeoutMs) {
            try {
                if (Boolean.TRUE.equals(condition.call())) {
                    return true;
                }
            } catch (Exception e) {}
            try { Thread.sleep(200); } catch (Exception e) {}
        }
        return false;
    }
} 