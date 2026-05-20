package com.yourname.minisql.region.master;

import com.yourname.minisql.region.network.NetworkConst;
import com.yourname.minisql.region.network.protocol.Message;
import com.yourname.minisql.region.zk.ServiceDiscovery;
import com.yourname.minisql.region.zk.ZkClientManager;
import com.yourname.minisql.region.zk.ZkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

public class MasterServer {
    private static final Logger log = LoggerFactory.getLogger(MasterServer.class);
    
    private final int port;
    private ServerSocket serverSocket;
    private final ExecutorService threadPool;
    private volatile boolean running = true;
    
    // 服务发现
    private ServiceDiscovery serviceDiscovery;
    
    // 静态路由表：表名 -> Region 地址（从 ZK 动态获取）
    private final Map<String, String> tableToRegion = new ConcurrentHashMap<>();
    
    public MasterServer(int port) {
        this.port = port;
        this.threadPool = Executors.newCachedThreadPool();
    }
    
    public void start() throws IOException {
        // 初始化服务发现
        try {
            serviceDiscovery = new ServiceDiscovery();
            serviceDiscovery.start();
            
            // 监听 Region 变化
            serviceDiscovery.addListener(new ServiceDiscovery.RegionChangeListener() {
                @Override
                public void onRegionOnline(ZkConfig.RegionData region) {
                    log.info("Region online: {}", region.getAddress());
                    // 更新路由映射（可选的亲和性策略）
                }
                
                @Override
                public void onRegionOffline(ZkConfig.RegionData region) {
                    log.info("Region offline: {}", region.getAddress());
                    // 从路由表中移除该 Region 的表
                    removeRegionFromRouting(region.getAddress());
                }
            });
            
            log.info("ServiceDiscovery initialized");
        } catch (Exception e) {
            log.error("Failed to initialize ServiceDiscovery", e);
            throw new IOException("Failed to initialize ZK discovery", e);
        }
        
        serverSocket = new ServerSocket(port);
        log.info("Master server started on port {}", port);
        
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                threadPool.submit(new MasterHandler(clientSocket, tableToRegion, serviceDiscovery));
            } catch (IOException e) {
                if (running) {
                    log.error("Error accepting connection", e);
                }
            }
        }
    }
    
    private void removeRegionFromRouting(String regionAddress) {
        // 从路由表中移除该 Region 的所有表
        List<String> tablesToRemove = new ArrayList<>();
        for (Map.Entry<String, String> entry : tableToRegion.entrySet()) {
            if (entry.getValue().equals(regionAddress)) {
                tablesToRemove.add(entry.getKey());
            }
        }
        for (String table : tablesToRemove) {
            tableToRegion.remove(table);
            log.info("Removed routing: {} -> {}", table, regionAddress);
        }
    }
    
    public void stop() {
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            log.error("Error stopping master", e);
        }
        threadPool.shutdownNow();
        if (serviceDiscovery != null) {
            try {
                serviceDiscovery.close();
            } catch (IOException e) {
                log.error("Error closing service discovery", e);
            }
        }
    }
    
    private static class MasterHandler implements Runnable {
        private final Socket socket;
        private final Map<String, String> tableToRegion;
        private final ServiceDiscovery serviceDiscovery;
        
        public MasterHandler(Socket socket, Map<String, String> tableToRegion, 
                            ServiceDiscovery serviceDiscovery) {
            this.socket = socket;
            this.tableToRegion = tableToRegion;
            this.serviceDiscovery = serviceDiscovery;
        }
        
        @Override
        public void run() {
            try (DataInputStream dis = new DataInputStream(socket.getInputStream());
                 DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {
                
                int length = dis.readInt();
                byte[] data = new byte[length];
                dis.readFully(data);
                
                Message msg = Message.decode(data);
                log.info("Master received request: type={}, requestId={}", 
                        msg.getType(), msg.getRequestId());
                
                String body = msg.getBodyAsString();
                String[] parts = body.split("\\s+", 2);
                if (parts.length < 2) {
                    sendErrorResponse(dos, msg.getRequestId(), "Invalid request format");
                    return;
                }
                
                int requestType = Integer.parseInt(parts[0]);
                String tableName = parts[1];
                
                Message response;
                if (requestType == NetworkConst.RequestType.GET_REGION) {
                    // 优先从路由表获取，如果没有则从服务发现获取
                    String regionAddr = tableToRegion.getOrDefault(
                        tableName, 
                        serviceDiscovery.getRegionByTable(tableName)
                    );
                    if (regionAddr == null) {
                        regionAddr = serviceDiscovery.getNextRegionRoundRobin();
                    }
                    if (regionAddr == null) {
                        sendErrorResponse(dos, msg.getRequestId(), "No available region");
                        return;
                    }
                    response = Message.createResponse(
                        msg.getRequestId(),
                        NetworkConst.Status.SUCCESS,
                        regionAddr.getBytes()
                    );
                    
                } else if (requestType == NetworkConst.RequestType.CREATE_TABLE) {
                    String assignedRegion = serviceDiscovery.getNextRegionRoundRobin();
                    if (assignedRegion == null) {
                        sendErrorResponse(dos, msg.getRequestId(), "No available region");
                        return;
                    }
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
                
                byte[] respData = response.encode();
                dos.writeInt(respData.length);
                dos.write(respData);
                dos.flush();
                
            } catch (Exception e) {
                log.error("Error handling master request", e);
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    log.error("Error closing socket", e);
                }
            }
        }
        
        private void sendErrorResponse(DataOutputStream dos, long requestId, String errorMsg) throws IOException {
            Message errorResponse = Message.createResponse(
                requestId,
                NetworkConst.Status.ERROR,
                errorMsg.getBytes()
            );
            byte[] respData = errorResponse.encode();
            dos.writeInt(respData.length);
            dos.write(respData);
            dos.flush();
        }
    }
    
    public static void main(String[] args) throws IOException {
        MasterServer master = new MasterServer(NetworkConst.MASTER_PORT);
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down MasterServer...");
            master.stop();
        }));
        
        master.start();
    }
}