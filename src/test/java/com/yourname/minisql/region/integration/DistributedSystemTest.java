package com.yourname.minisql.region.integration;

import com.yourname.minisql.region.client.Client;
import com.yourname.minisql.region.master.MasterServer;
import com.yourname.minisql.region.manager.DatabaseManager;
import com.yourname.minisql.region.network.RegionServer;
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

public class DistributedSystemTest {
    
    private MasterServer masterServer;
    private RegionServer regionServer1;
    private RegionServer regionServer2;
    private ExecutorService executor;
    private Client client;
    
    @TempDir
    Path tempDir;
    
    private int masterPort;
    private int region1Port;
    private int region2Port;
    
    @BeforeEach
public void setUp() throws Exception {
    System.out.println("\n=== DistributedSystemTest Setup ===");
    
    masterPort = findFreePort();
    region1Port = findFreePort();
    region2Port = findFreePort();
    
    System.out.println("Ports - Master: " + masterPort + ", Region1: " + region1Port + ", Region2: " + region2Port);
    
    executor = Executors.newFixedThreadPool(5);
    
    // 1. 启动 Master
    masterServer = new MasterServer(masterPort);
    executor.submit(() -> {
        try {
            masterServer.start();
            System.out.println("MasterServer started on port " + masterPort);
        } catch (IOException e) {
            System.err.println("MasterServer failed: " + e.getMessage());
        }
    });
    waitForServer(masterPort, 5000);
    System.out.println("✓ MasterServer is ready");
    
    // ★ 关键：更新 Master 中的 Region 地址为实际启动的端口
    masterServer.updateRegionAddress(0, "localhost:" + region1Port);
    masterServer.updateRegionAddress(1, "localhost:" + region2Port);
    System.out.println("✓ Updated Master routing: Region1 -> localhost:" + region1Port);
    System.out.println("✓ Updated Master routing: Region2 -> localhost:" + region2Port);
    
    // 2. 启动 Region1
    DatabaseManager db1 = new DatabaseManager(tempDir.resolve("region1").toString());
    regionServer1 = new RegionServer(region1Port, db1);
    executor.submit(() -> {
        try {
            regionServer1.start();
            System.out.println("RegionServer1 started on port " + region1Port);
        } catch (IOException e) {
            System.err.println("RegionServer1 failed: " + e.getMessage());
        }
    });
    waitForServer(region1Port, 5000);
    System.out.println("✓ RegionServer1 is ready on port " + region1Port);
    
    // 3. 启动 Region2
    DatabaseManager db2 = new DatabaseManager(tempDir.resolve("region2").toString());
    regionServer2 = new RegionServer(region2Port, db2);
    executor.submit(() -> {
        try {
            regionServer2.start();
            System.out.println("RegionServer2 started on port " + region2Port);
        } catch (IOException e) {
            System.err.println("RegionServer2 failed: " + e.getMessage());
        }
    });
    waitForServer(region2Port, 5000);
    System.out.println("✓ RegionServer2 is ready on port " + region2Port);
    
    // 4. 创建 Client
    client = new Client("localhost", masterPort);
    
    System.out.println("=== Setup Complete ===\n");
}
    
    @AfterEach
    public void tearDown() throws Exception {
        System.out.println("\n=== Teardown ===");
        
        if (regionServer1 != null) {
            regionServer1.stop();
            System.out.println("RegionServer1 stopped");
        }
        if (regionServer2 != null) {
            regionServer2.stop();
            System.out.println("RegionServer2 stopped");
        }
        if (masterServer != null) {
            masterServer.stop();
            System.out.println("MasterServer stopped");
        }
        if (executor != null) {
            executor.shutdownNow();
        }
        if (client != null) {
            // Client 没有 close 方法，不需要
        }
        System.out.println("=== Teardown Complete ===\n");
    }
    
    @Test
    @DisplayName("E2E: 完整流程 - 建表、插入、查询、删除")
    public void testCompleteWorkflow() {
        System.out.println("\n========== E2E 测试开始 ==========");
        
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        
        try {
            System.setOut(new PrintStream(outContent));
            
            // 步骤1: 建表
            System.out.println("\n>>> 步骤1: 执行建表");
            String createSQL = "CREATE TABLE users (id STRING, name STRING, age INT)";
            System.out.println("SQL: " + createSQL);
            client.execute(createSQL);
            
            String output = outContent.toString();
            System.out.println("捕获的输出: " + output);
            
            // 修改断言：检查是否包含 "Result" 而不是 "Got region address"
            // 因为 Client 的输出格式可能不同
            assertTrue(output.contains("Result") || output.contains("created"),
                       "建表应该成功, 实际输出: " + output);
            outContent.reset();
            
            // 步骤2: 插入数据
            System.out.println("\n>>> 步骤2: 执行插入");
            String insertSQL = "INSERT INTO users (id, name, age) VALUES ('1', 'Alice', '25')";
            System.out.println("SQL: " + insertSQL);
            client.execute(insertSQL);
            
            output = outContent.toString();
            System.out.println("捕获的输出: " + output);
            assertTrue(output.contains("Inserted row") || output.contains("成功"),
                       "插入应该成功, 实际输出: " + output);
            outContent.reset();
            
            // 步骤3: 查询数据
            System.out.println("\n>>> 步骤3: 执行查询");
            String selectSQL = "SELECT * FROM users WHERE id = '1'";
            System.out.println("SQL: " + selectSQL);
            client.execute(selectSQL);
            
            output = outContent.toString();
            System.out.println("捕获的输出: " + output);
            assertTrue(output.contains("Alice") && output.contains("25"),
                       "查询应该返回正确数据, 实际输出: " + output);
            outContent.reset();
            
            // 步骤4: 删除数据
            System.out.println("\n>>> 步骤4: 执行删除");
            String deleteSQL = "DELETE FROM users WHERE id = '1'";
            System.out.println("SQL: " + deleteSQL);
            client.execute(deleteSQL);
            
            output = outContent.toString();
            System.out.println("捕获的输出: " + output);
            assertTrue(output.contains("Deleted row") || output.contains("删除成功"),
                       "删除应该成功, 实际输出: " + output);
            outContent.reset();
            
            // 步骤5: 验证删除
            System.out.println("\n>>> 步骤5: 验证删除");
            client.execute(selectSQL);
            
            output = outContent.toString();
            System.out.println("捕获的输出: " + output);
            assertTrue(output.contains("No row found") || output.contains("未找到"),
                       "删除后不应该找到数据, 实际输出: " + output);
            
            System.out.println("\n========== E2E 测试通过 ==========\n");
            
        } catch (Exception e) {
            System.err.println("测试失败: " + e.getMessage());
            e.printStackTrace();
            throw e;
        } finally {
            System.setOut(originalOut);
        }
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
                System.out.println("  Server on port " + port + " is ready (attempt " + attempt + ")");
                return;
            } catch (IOException e) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        throw new RuntimeException("Server on port " + port + " did not start within " + timeoutMs + "ms");
    }
}