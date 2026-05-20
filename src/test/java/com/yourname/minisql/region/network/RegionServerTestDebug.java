package com.yourname.minisql.region.network;

import com.yourname.minisql.region.manager.DatabaseManager;
import com.yourname.minisql.region.network.protocol.Message;
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

public class RegionServerTestDebug {
    
    private RegionServer regionServer;
    private DatabaseManager dbManager;
    private ExecutorService executor;
    private int regionPort;
    
    @TempDir
    Path tempDir;
    
    @BeforeEach
    public void setUp() throws Exception {
        System.out.println("\n=== setUp 开始 ===");
        regionPort = findFreePort();
        System.out.println("找到空闲端口: " + regionPort);
        
        System.out.println("创建 DatabaseManager，数据目录: " + tempDir.toString());
        dbManager = new DatabaseManager(tempDir.toString());
        
        System.out.println("创建 RegionServer，端口: " + regionPort);
        regionServer = new RegionServer(regionPort, dbManager);
        
        executor = Executors.newSingleThreadExecutor();
        
        System.out.println("启动 RegionServer...");
        executor.submit(() -> {
            try {
                regionServer.start();
                System.out.println("RegionServer 启动成功");
            } catch (IOException e) {
                System.err.println("RegionServer 启动失败: " + e.getMessage());
                e.printStackTrace();
            }
        });
        
        System.out.println("等待服务器启动...");
        waitForServer(regionPort, 5000);
        System.out.println("服务器已启动，端口: " + regionPort);
        System.out.println("=== setUp 完成 ===\n");
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        System.out.println("\n=== tearDown 开始 ===");
        if (regionServer != null) {
            System.out.println("停止 RegionServer...");
            regionServer.stop();
        }
        if (dbManager != null) {
            System.out.println("关闭 DatabaseManager...");
            dbManager.close();
        }
        if (executor != null) {
            System.out.println("关闭线程池...");
            executor.shutdownNow();
        }
        System.out.println("=== tearDown 完成 ===\n");
    }
    
    @Test
    @DisplayName("测试 Region 接收并执行 CREATE TABLE")
    public void testCreateTable() throws IOException {
        System.out.println("\n>>> 测试 CREATE TABLE <<<");
        String sql = "CREATE TABLE users (id STRING, name STRING, age INT)";
        System.out.println("发送 SQL: " + sql);
        
        String response = sendSqlToRegion(sql);
        System.out.println("收到响应: '" + response + "'");
        
        assertNotNull(response, "响应不应为 null");
        assertTrue(response.contains("Table 'users' created") || 
                   response.contains("created") ||
                   response.contains("创建"),
                   "响应应包含创建确认信息，实际响应: " + response);
        System.out.println(">>> 测试通过 <<<\n");
    }
    
    @Test
    @DisplayName("测试 Region 接收并执行 INSERT")
    public void testInsert() throws IOException {
        System.out.println("\n>>> 测试 INSERT <<<");
        
        System.out.println("步骤1: 创建表");
        String createResponse = sendSqlToRegion("CREATE TABLE users (id STRING, name STRING, age INT)");
        System.out.println("建表响应: " + createResponse);
        
        System.out.println("步骤2: 插入数据");
        String sql = "INSERT INTO users (id, name, age) VALUES ('1', 'Alice', '25')";
        System.out.println("发送 SQL: " + sql);
        
        String response = sendSqlToRegion(sql);
        System.out.println("收到响应: '" + response + "'");
        
        assertNotNull(response, "响应不应为 null");
        assertTrue(response.contains("Inserted row with key: 1") || 
                   response.contains("插入成功"),
                   "插入应返回成功信息，实际响应: " + response);
        System.out.println(">>> 测试通过 <<<\n");
    }
    
    @Test
    @DisplayName("测试 Region 接收并执行 SELECT")
    public void testSelect() throws IOException {
        System.out.println("\n>>> 测试 SELECT <<<");
        
        System.out.println("步骤1: 创建表");
        sendSqlToRegion("CREATE TABLE users (id STRING, name STRING, age INT)");
        
        System.out.println("步骤2: 插入数据");
        sendSqlToRegion("INSERT INTO users (id, name, age) VALUES ('1', 'Alice', '25')");
        
        System.out.println("步骤3: 查询数据");
        String sql = "SELECT * FROM users WHERE id = '1'";
        System.out.println("发送 SQL: " + sql);
        
        String response = sendSqlToRegion(sql);
        System.out.println("收到响应: '" + response + "'");
        
        assertNotNull(response, "响应不应为 null");
        assertTrue(response.contains("Alice"), "响应应包含 'Alice'，实际响应: " + response);
        assertTrue(response.contains("25"), "响应应包含 '25'，实际响应: " + response);
        System.out.println(">>> 测试通过 <<<\n");
    }
    
    @Test
    @DisplayName("测试 Region 接收并执行 DELETE")
    public void testDelete() throws IOException {
        System.out.println("\n>>> 测试 DELETE <<<");
        
        System.out.println("步骤1: 创建表");
        sendSqlToRegion("CREATE TABLE users (id STRING, name STRING, age INT)");
        
        System.out.println("步骤2: 插入数据");
        sendSqlToRegion("INSERT INTO users (id, name, age) VALUES ('1', 'Alice', '25')");
        
        System.out.println("步骤3: 删除数据");
        String deleteSql = "DELETE FROM users WHERE id = '1'";
        System.out.println("发送 SQL: " + deleteSql);
        String deleteResponse = sendSqlToRegion(deleteSql);
        System.out.println("删除响应: '" + deleteResponse + "'");
        assertTrue(deleteResponse.contains("Deleted row") || deleteResponse.contains("删除成功"),
                   "删除应成功，实际响应: " + deleteResponse);
        
        System.out.println("步骤4: 验证删除");
        String selectResponse = sendSqlToRegion("SELECT * FROM users WHERE id = '1'");
        System.out.println("查询响应: '" + selectResponse + "'");
        assertTrue(selectResponse.contains("No row found") || selectResponse.contains("未找到"),
                   "删除后不应找到数据，实际响应: " + selectResponse);
        System.out.println(">>> 测试通过 <<<\n");
    }
    
    @Test
    @DisplayName("测试连接性 - 简单 Ping")
    public void testConnectivity() throws IOException {
        System.out.println("\n>>> 测试连接性 <<<");
        
        System.out.println("尝试连接到 localhost:" + regionPort);
        try (Socket socket = new Socket("localhost", regionPort)) {
            System.out.println("连接成功！");
            assertTrue(socket.isConnected(), "应该能够连接到服务器");
        }
        System.out.println(">>> 测试通过 <<<\n");
    }
    
    private String sendSqlToRegion(String sql) throws IOException {
        System.out.println("  [发送] 编码消息...");
        Message request = Message.createRequest(1L, NetworkConst.MessageType.REQUEST, sql.getBytes());
        byte[] reqData = request.encode();
        System.out.println("  [发送] 消息长度: " + reqData.length);
        
        try (Socket socket = new Socket("localhost", regionPort);
             DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
             DataInputStream dis = new DataInputStream(socket.getInputStream())) {
            
            System.out.println("  [发送] 已连接到服务器");
            
            dos.writeInt(reqData.length);
            dos.write(reqData);
            dos.flush();
            System.out.println("  [发送] 已发送请求");
            
            System.out.println("  [接收] 等待响应...");
            int len = dis.readInt();
            System.out.println("  [接收] 响应长度: " + len);
            
            byte[] respData = new byte[len];
            dis.readFully(respData);
            System.out.println("  [接收] 已读取响应数据");
            
            Message response = Message.decode(respData);
            String body = response.getBodyAsString();
            System.out.println("  [接收] 响应内容: " + body);
            
            return body;
        } catch (IOException e) {
            System.err.println("  [错误] 发送请求失败: " + e.getMessage());
            e.printStackTrace();
            throw e;
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
            System.out.println("  尝试连接 (attempt " + attempt + ") 到端口 " + port);
            try (Socket socket = new Socket("localhost", port)) {
                System.out.println("  连接成功！");
                return;
            } catch (IOException e) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        throw new RuntimeException("Server did not start within " + timeoutMs + "ms on port " + port);
    }
}