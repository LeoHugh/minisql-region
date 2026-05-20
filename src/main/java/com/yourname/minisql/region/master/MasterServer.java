package com.yourname.minisql.region.master;

import com.yourname.minisql.region.network.NetworkConst;
import com.yourname.minisql.region.network.protocol.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MasterServer {
    private static final Logger log = LoggerFactory.getLogger(MasterServer.class);
    
    private final int port;
    private ServerSocket serverSocket;
    private final ExecutorService threadPool;
    private volatile boolean running = true;

public void stop() {
    running = false;
    try {
        if (serverSocket != null) {
            serverSocket.close();
        }
    } catch (IOException e) {
        log.error("Error stopping master", e);
    }
    threadPool.shutdownNow();
}
    // 静态路由表：表名 -> Region 地址
    private final Map<String, String> tableToRegion = new ConcurrentHashMap<>();
    // 静态配置的 Region 列表（写死几个用于测试）
    private final List<String> regionAddresses = Arrays.asList(
        "localhost:8888",   // Region 1
        "localhost:8889"    // Region 2（可以启动第二个 Region）
    );
    
    public MasterServer(int port) {
        this.port = port;
        this.threadPool = Executors.newCachedThreadPool();
        initStaticRouting();
    }
    
    private void initStaticRouting() {
        // 静态路由：写死哪些表路由到哪个 Region
        tableToRegion.put("users", regionAddresses.get(0));
        tableToRegion.put("orders", regionAddresses.get(1));
        // 默认路由到第一个 Region
        log.info("Static routing initialized: {}", tableToRegion);
    }
    
    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        log.info("Master server started on port {}", port);
        
        while (true) {
            Socket clientSocket = serverSocket.accept();
            threadPool.submit(new MasterHandler(clientSocket, tableToRegion, regionAddresses));
        }
    }
    
    private static class MasterHandler implements Runnable {
        private final Socket socket;
        private final Map<String, String> tableToRegion;
        private final List<String> regionAddresses;
        
        public MasterHandler(Socket socket, Map<String, String> tableToRegion, List<String> regionAddresses) {
            this.socket = socket;
            this.tableToRegion = tableToRegion;
            this.regionAddresses = regionAddresses;
        }
        
        @Override
        public void run() {
            try (DataInputStream dis = new DataInputStream(socket.getInputStream());
                 DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {
                
                int length = dis.readInt();
                byte[] data = new byte[length];
                dis.readFully(data);
                
                Message msg = Message.decode(data);
                log.info("Master received request: type={}", msg.getType());
                
                // 解析请求体格式：[requestType] [tableName] 或其他
                String body = msg.getBodyAsString();
                String[] parts = body.split("\\s+", 2);
                int requestType = Integer.parseInt(parts[0]);
                
                Message response;
                if (requestType == NetworkConst.RequestType.GET_REGION) {
                    // 获取 Region 地址
                    String tableName = parts[1];
                    String regionAddr = tableToRegion.getOrDefault(tableName, regionAddresses.get(0));
                    response = Message.createResponse(
                        msg.getRequestId(),
                        NetworkConst.Status.SUCCESS,
                        regionAddr.getBytes()
                    );
                } else if (requestType == NetworkConst.RequestType.CREATE_TABLE) {
                    // 建表请求：记录表到 Region 的映射
                    String tableName = parts[1];
                    // 简单策略：分配到第一个 Region
                    String assignedRegion = regionAddresses.get(0);
                    tableToRegion.put(tableName, assignedRegion);
                    response = Message.createResponse(
                        msg.getRequestId(),
                        NetworkConst.Status.SUCCESS,
                        assignedRegion.getBytes()
                    );
                } else {
                    response = Message.createResponse(
                        msg.getRequestId(),
                        NetworkConst.Status.ERROR,
                        "Unknown request type".getBytes()
                    );
                }
                
                dos.writeInt(response.encode().length);
                dos.write(response.encode());
                dos.flush();
            } catch (IOException e) {
                log.error("Error handling master request", e);
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    log.error("Error closing socket", e);
                }
            }
        }
    }
    
    public static void main(String[] args) throws IOException {
        MasterServer master = new MasterServer(NetworkConst.MASTER_PORT);
        master.start();
    }


     public void updateRegionAddress(int index, String address) {
        if (index == 0 && regionAddresses.size() > 0) {
            regionAddresses.set(0, address);
        } else if (index == 1 && regionAddresses.size() > 1) {
            regionAddresses.set(1, address);
        }
        // 同时更新已存在的表映射
        for (Map.Entry<String, String> entry : tableToRegion.entrySet()) {
            if (entry.getValue().equals("localhost:8888") && index == 0) {
                tableToRegion.put(entry.getKey(), address);
            } else if (entry.getValue().equals("localhost:8889") && index == 1) {
                tableToRegion.put(entry.getKey(), address);
            }
        }
        log.info("Updated region address: index={}, address={}", index, address);
    }
}