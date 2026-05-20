package com.yourname.minisql.region.master;

import com.yourname.minisql.region.client.Client;
import com.yourname.minisql.region.manager.DatabaseManager;
import com.yourname.minisql.region.network.NetworkConst;
import com.yourname.minisql.region.network.RegionServer;
import com.yourname.minisql.region.network.protocol.Message;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.net.ServerSocket;
import java.io.*;
import java.net.Socket;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class MasterServerTest {
    
    private MasterServer masterServer;
    private ExecutorService executor;
    private int masterPort;
    private int region1Port,region2Port;
    @BeforeEach
    public void setUp() throws IOException {
        masterPort = findFreePort();
        masterServer = new MasterServer(masterPort);
        executor = Executors.newSingleThreadExecutor();
        
        // 在独立线程中启动 Master
        executor.submit(() -> {
            try {
                masterServer.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        
        // 等待服务器启动
        waitForServer(masterPort, 5000);
    }
    
    
    @AfterEach
    public void tearDown() {
        if (masterServer != null) {
            try {
                // 停止服务器（需要添加 stop 方法）
                masterServer.stop();
            } catch (Exception e) {
                // ignore
            }
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
@DisplayName("测试 Master 响应 CREATE_TABLE 请求")
public void testCreateTableRequest() throws IOException {
    System.out.println("\n>>> Test: CREATE_TABLE Request <<<");
    
    String tableName = "test_table";
    String body = NetworkConst.RequestType.CREATE_TABLE + " " + tableName;
    Message request = Message.createRequest(1L, NetworkConst.MessageType.REQUEST, body.getBytes());
    byte[] reqData = request.encode();
    
    try (Socket socket = new Socket("localhost", masterPort);
         DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
         DataInputStream dis = new DataInputStream(socket.getInputStream())) {
        
        dos.writeInt(reqData.length);
        dos.write(reqData);
        dos.flush();
        
        int len = dis.readInt();
        byte[] respData = new byte[len];
        dis.readFully(respData);
        
        Message response = Message.decode(respData);
        
        assertEquals(NetworkConst.Status.SUCCESS, response.getStatus());
        
        // 修改断言：CREATE_TABLE 应该返回 Region 地址，而不是表名
        String responseBody = response.getBodyAsString();
        System.out.println("Response body: '" + responseBody + "'");
        assertTrue(responseBody.contains("localhost:") || responseBody.contains("127.0.0.1"),
                   "Response should be a region address, but got: " + responseBody);
        
        System.out.println(">>> Test PASSED <<<\n");
    }
}
    @Test
    @DisplayName("测试 Master 响应 GET_REGION 请求")
    public void testGetRegionRequest() throws IOException {
        // 发送 GET_REGION 请求
        String body = NetworkConst.RequestType.GET_REGION + " users";
        Message request = Message.createRequest(1L, NetworkConst.MessageType.REQUEST, body.getBytes());
        
        try (Socket socket = new Socket("localhost", masterPort);
             DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
             DataInputStream dis = new DataInputStream(socket.getInputStream())) {
            
            byte[] reqData = request.encode();
            dos.writeInt(reqData.length);
            dos.write(reqData);
            dos.flush();
            
            int len = dis.readInt();
            byte[] respData = new byte[len];
            dis.readFully(respData);
            
            Message response = Message.decode(respData);
            
            assertEquals(NetworkConst.Status.SUCCESS, response.getStatus());
            String regionAddr = response.getBodyAsString();
            assertNotNull(regionAddr);
            assertTrue(regionAddr.contains("localhost:"));
        }
    }
    
    
    
    @Test
    @DisplayName("测试未知表的路由（默认 Region）")
    public void testUnknownTableRouting() throws IOException {
        String body = NetworkConst.RequestType.GET_REGION + " unknown_table";
        Message request = Message.createRequest(1L, NetworkConst.MessageType.REQUEST, body.getBytes());
        
        try (Socket socket = new Socket("localhost", masterPort);
             DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
             DataInputStream dis = new DataInputStream(socket.getInputStream())) {
            
            byte[] reqData = request.encode();
            dos.writeInt(reqData.length);
            dos.write(reqData);
            dos.flush();
            
            int len = dis.readInt();
            byte[] respData = new byte[len];
            dis.readFully(respData);
            
            Message response = Message.decode(respData);
            
            assertEquals(NetworkConst.Status.SUCCESS, response.getStatus());
            String regionAddr = response.getBodyAsString();
            assertNotNull(regionAddr);
            // 应该返回默认 Region
        }
    }
    
    private int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
    
    private void waitForServer(int port, int timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (Socket socket = new Socket("localhost", port)) {
                return;
            } catch (IOException e) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        throw new RuntimeException("Server did not start within " + timeoutMs + "ms");
    }
}