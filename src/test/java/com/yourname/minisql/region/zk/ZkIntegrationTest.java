package com.yourname.minisql.region.zk;

import com.yourname.minisql.region.client.Client;
import com.yourname.minisql.region.master.MasterServer;
import com.yourname.minisql.region.network.RegionServer;
import com.yourname.minisql.region.manager.DatabaseManager;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Zookeeper 端到端集成测试
 * 验证完整的服务注册、发现、路由流程
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ZkIntegrationTest {
    
    private MasterServer masterServer;
    private RegionServer regionServer1;
    private RegionServer regionServer2;
    private RegionRegistry regionRegistry1;
    private RegionRegistry regionRegistry2;
    private ExecutorService executor;
    private Client client;
    private ZkClientManager zkClient;
    
    @TempDir
    Path tempDir;
    
    private int masterPort;
    private int region1Port;
    private int region2Port;
    
    @BeforeEach
    public void setUp() throws Exception {
        System.out.println("\n========== ZK 集成测试开始 ==========");
        
        // 初始化 ZK
        zkClient = ZkClientManager.getInstance();
        zkClient.init();
        
        // 清理 ZK 节点
        try {
            zkClient.deleteNode(ZkConfig.getGroupRegionsPath("test-group"));
        } catch (Exception e) {
            // 忽略
        }
        zkClient.ensurePathExists(ZkConfig.getGroupRegionsPath("test-group"));
        
        masterPort = findFreePort();
        region1Port = findFreePort();
        region2Port = findFreePort();
        
        System.out.println("端口分配:");
        System.out.println("  Master: " + masterPort);
        System.out.println("  Region1: " + region1Port);
        System.out.println("  Region2: " + region2Port);
        
        executor = Executors.newFixedThreadPool(10);
        
        // 1. 启动 Master（集成服务发现）
        masterServer = new MasterServer(masterPort);
        executor.submit(() -> {
            try {
                masterServer.start();
                System.out.println("MasterServer 已启动");
            } catch (IOException e) {
                System.err.println("MasterServer 启动失败: " + e.getMessage());
            }
        });
        waitForServer(masterPort, 10000);
        System.out.println("✓ MasterServer 就绪");
        
        // 2. 启动 Region1 并注册到 ZK
        DatabaseManager db1 = new DatabaseManager(tempDir.resolve("region1").toString());
        com.yourname.minisql.region.replication.ReplicationManager rep1 = new com.yourname.minisql.region.replication.ReplicationManager(db1, "region-1");
        db1.setReplicationManager(rep1);
        rep1.becomeMaster();
        regionServer1 = new RegionServer(region1Port, db1, rep1);
        regionRegistry1 = new RegionRegistry("localhost", region1Port, "test-group");
        regionRegistry1.register();
        regionRegistry1.updateReplicationInfo("MASTER", rep1.getReplicationPort());
        System.out.println("✓ Region1 已注册到 ZK");
        
        executor.submit(() -> {
            try {
                regionServer1.start();
                System.out.println("RegionServer1 已启动");
            } catch (IOException e) {
                System.err.println("RegionServer1 启动失败: " + e.getMessage());
            }
        });
        waitForServer(region1Port, 10000);
        System.out.println("✓ RegionServer1 就绪");
        
        // 3. 启动 Region2 并注册到 ZK
        DatabaseManager db2 = new DatabaseManager(tempDir.resolve("region2").toString());
        com.yourname.minisql.region.replication.ReplicationManager rep2 = new com.yourname.minisql.region.replication.ReplicationManager(db2, "region-2");
        db2.setReplicationManager(rep2);
        rep2.becomeSlave("localhost:" + rep1.getReplicationPort());
        regionServer2 = new RegionServer(region2Port, db2, rep2);
        regionRegistry2 = new RegionRegistry("localhost", region2Port, "test-group");
        regionRegistry2.register();
        regionRegistry2.updateReplicationInfo("SLAVE", rep2.getReplicationPort());
        System.out.println("✓ Region2 已注册到 ZK");
        
        executor.submit(() -> {
            try {
                regionServer2.start();
                System.out.println("RegionServer2 已启动");
            } catch (IOException e) {
                System.err.println("RegionServer2 启动失败: " + e.getMessage());
            }
        });
        waitForServer(region2Port, 10000);
        System.out.println("✓ RegionServer2 就绪");
        
        // 等待 ZK 同步和服务发现更新
        System.out.println("等待 RegionServer2 注册...");
        waitForCondition(() -> isRegionRegistered(region2Port), 5000);
        try { Thread.sleep(1000); } catch (Exception e) {}
        
        // 4. 创建 Client
        client = new Client("localhost", masterPort);
        
        System.out.println("========== 初始化完成 ==========\n");
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        System.out.println("\n========== 开始清理 ==========");
        
        if (regionRegistry1 != null) {
            regionRegistry1.close();
            System.out.println("RegionRegistry1 已关闭");
        }
        if (regionRegistry2 != null) {
            regionRegistry2.close();
            System.out.println("RegionRegistry2 已关闭");
        }
        if (regionServer1 != null) {
            regionServer1.stop();
            System.out.println("RegionServer1 已停止");
        }
        if (regionServer2 != null) {
            regionServer2.stop();
            System.out.println("RegionServer2 已停止");
        }
        if (masterServer != null) {
            masterServer.stop();
            System.out.println("MasterServer 已停止");
        }
        if (executor != null) {
            executor.shutdownNow();
        }
        
        System.out.println("========== 清理完成 ==========\n");
    }
    
    @Test
    @Order(1)
    @DisplayName("E2E: 验证 Region 自动注册到 ZK")
    public void testRegionAutoRegistration() throws Exception {
        System.out.println("\n>>> 测试 Region 自动注册");
        
        // 获取 ZK 中注册的节点
        java.util.List<String> children = zkClient.getChildren(ZkConfig.getGroupRegionsPath("test-group"));
        System.out.println("ZK 中的 Region 节点: " + children);
        
        assertTrue(children.contains("region-" + region1Port));
        assertTrue(children.contains("region-" + region2Port));
        
        System.out.println("✓ Region 自动注册测试通过");
    }
    
    @Test
    @Order(2)
    @DisplayName("E2E: 建表并通过 Master 路由到 Region")
    public void testCreateTableAndRoute() {
        System.out.println("\n>>> 测试建表和路由");
        
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        
        try {
            System.setOut(new PrintStream(outContent));
            
            String createSQL = "CREATE TABLE test_table (id STRING, name STRING, age INT)";
            System.out.println("执行: " + createSQL);
            client.execute(createSQL);
            
            String output = outContent.toString();
            System.out.println("输出: " + output);
            
            // 验证结果包含成功信息
            assertTrue(output.contains("Result") || output.contains("created"), 
                      "建表应成功，实际输出: " + output);
            
            System.out.println("✓ 建表和路由测试通过");
            
        } finally {
            System.setOut(originalOut);
        }
    }
    
    @Test
    @Order(3)
    @DisplayName("E2E: 完整 CRUD 操作")
    public void testFullCRUD() {
        System.out.println("\n>>> 测试完整 CRUD 操作");
        
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        
        try {
            System.setOut(new PrintStream(outContent));
            
            // 建表
            client.execute("CREATE TABLE test_crud (id STRING, name STRING, age INT)");
            outContent.reset();
            
            // 插入
            client.execute("INSERT INTO test_crud (id, name, age) VALUES ('1', 'Alice', '25')");
            String output = outContent.toString();
            assertTrue(output.contains("Inserted row") || output.contains("成功"), 
                      "插入应成功");
            outContent.reset();
            
            
            
            // 查询
            client.execute("SELECT * FROM test_crud WHERE id = '1'");
            output = outContent.toString();
            assertTrue(output.contains("Alice") && output.contains("25"), 
                      "查询应返回正确数据");
            outContent.reset();
            
            // 删除
            client.execute("DELETE FROM test_crud WHERE id = '1'");
            output = outContent.toString();
            assertTrue(output.contains("Deleted row") || output.contains("删除成功"), 
                      "删除应成功");
            outContent.reset();
            
            
            
            // 验证删除
            client.execute("SELECT * FROM test_crud WHERE id = '1'");
            output = outContent.toString();
            assertTrue(output.contains("No row found") || output.contains("未找到"), 
                      "删除后不应找到数据");
            
            System.out.println("✓ 完整 CRUD 操作测试通过");
            
        } finally {
            System.setOut(originalOut);
        }
    }
    
    @Test
    @Order(4)
    @DisplayName("E2E: 验证 Region 下线后 Master 不再路由")
    public void testRegionOfflineHandling() throws Exception {
        System.out.println("\n>>> 测试 Region 下线处理");
        
        // 关闭 Region2
        if (regionRegistry2 != null) {
            regionRegistry2.close();
            System.out.println("Region2 已注销");
        }
        if (regionServer2 != null) {
            regionServer2.stop();
            System.out.println("Region2 已停止");
        }
        
        // 等待 ZK 感知
        System.out.println("等待 RegionServer2 下线感知...");
        waitForCondition(() -> !isRegionRegistered(region2Port), 5000);
        try { Thread.sleep(1000); } catch (Exception e) {}
        
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        
        try {
            System.setOut(new PrintStream(outContent));
            
            // 创建新表
            client.execute("CREATE TABLE after_offline (id STRING, name STRING)");
            String output = outContent.toString();
            
            // 验证仍然可以操作
            assertTrue(output.contains("Result") || output.contains("created"), 
                      "下线后仍应能操作");
            
            System.out.println("✓ Region 下线后 Master 正确处理");
            
        } finally {
            System.setOut(originalOut);
        }
    }
    
    @Test
    @Order(5)
    @DisplayName("E2E: 验证 Region 重新上线后自动恢复")
    public void testRegionReOnline() throws Exception {
        System.out.println("\n>>> 测试 Region 重新上线");
        
        // 先关闭 Region2
        if (regionRegistry2 != null) {
            regionRegistry2.close();
            System.out.println("Region2 已注销");
        }
        
        Thread.sleep(2000);
        
        // 重新启动 Region2
        DatabaseManager db2 = new DatabaseManager(tempDir.resolve("region2_re").toString());
        regionServer2 = new RegionServer(region2Port, db2);
        regionRegistry2 = new RegionRegistry("localhost", region2Port, "test-group");
        regionRegistry2.register();
        regionRegistry2.updateReplicationInfo("SLAVE", 0);
        System.out.println("Region2 重新注册");
        
        executor.submit(() -> {
            try {
                regionServer2.start();
            } catch (IOException e) {
                System.err.println("RegionServer2 重启失败: " + e.getMessage());
            }
        });
        waitForServer(region2Port, 10000);
        
        Thread.sleep(3000);
        
        // 验证 ZK 中节点存在
        java.util.List<String> children = zkClient.getChildren(ZkConfig.getGroupRegionsPath("test-group"));
        System.out.println("ZK 中的 Region 节点: " + children);
        assertTrue(children.contains("region-" + region2Port), 
                  "Region2 应重新注册到 ZK");
        
        System.out.println("✓ Region 重新上线后自动恢复测试通过");
    }
    
    private int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
    
    private void waitForServer(int port, int timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        int attempt = 0;
        while (System.currentTimeMillis() < deadline) {
            attempt++;
            try (Socket socket = new Socket("localhost", port)) {
                System.out.println("  端口 " + port + " 已就绪 (尝试 " + attempt + ")");
                return;
            } catch (IOException e) {
                try {
                    Thread.sleep(300);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        throw new RuntimeException("端口 " + port + " 在 " + timeoutMs + "ms 内未能启动");
    }

    private boolean isRegionRegistered(int port) {
        try {
            java.util.List<String> children = zkClient.getChildren(ZkConfig.getGroupRegionsPath("test-group"));
            return children.contains("region-" + port);
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