package com.yourname.minisql.region.master;

import com.yourname.minisql.region.loadbalance.LoadBalancer;
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
import java.util.concurrent.CopyOnWriteArrayList;

public class MasterServer {
    private static final Logger log = LoggerFactory.getLogger(MasterServer.class);
    
    private final int port;
    private ServerSocket serverSocket;
    private final ExecutorService threadPool;
    private volatile boolean running = true;
    
    // 服务发现
    private ServiceDiscovery serviceDiscovery;
    
    // 负载均衡器
    private final LoadBalancer loadBalancer;
    
    // 静态路由表：表名 -> Region 地址（从 ZK 动态获取）
    private final Map<String, String> tableToRegion = new ConcurrentHashMap<>();
    
    // 在 start() 之前注册的额外监听器（如 RegionFailover）
    private final List<ServiceDiscovery.RegionChangeListener> pendingListeners = new CopyOnWriteArrayList<>();
    
    public MasterServer(int port) {
        this(port, new LoadBalancer());
    }
    
    public MasterServer(int port, LoadBalancer loadBalancer) {
        this.port = port;
        this.loadBalancer = loadBalancer;
        this.threadPool = Executors.newCachedThreadPool();
    }
    
    /**
     * 注册额外的 Region 变化监听器（需在 start() 之前调用）。
     * 用于让 RegionFailover 等外部组件复用 ServiceDiscovery 的 ZK 事件，
     * 避免重复创建 CuratorCache。
     */
    public void addRegionChangeListener(ServiceDiscovery.RegionChangeListener listener) {
        pendingListeners.add(listener);
    }
    
    public void start() throws IOException {
        // 初始化服务发现
        try {
            serviceDiscovery = new ServiceDiscovery();
            
            // 先注册所有外部监听器（如 RegionFailover），确保它们能接收到已有节点的事件
            for (ServiceDiscovery.RegionChangeListener listener : pendingListeners) {
                serviceDiscovery.addListener(listener);
                log.info("Registered external region change listener: {}", listener.getClass().getSimpleName());
            }
            
            // 注册内部监听器（LoadBalancer 同步）
            serviceDiscovery.addListener(new ServiceDiscovery.RegionChangeListener() {
                @Override
                public void onRegionOnline(ZkConfig.RegionData region) {
                    String address = region.getAddress();
                    String regionId = region.getHost() + ":" + region.getPort();
                    log.info("Region online: {}", address);
                    loadBalancer.addRegion(regionId, address);
                    loadBalancer.markAvailable(address, true);
                }
                
                @Override
                public void onRegionOffline(ZkConfig.RegionData region) {
                    String address = region.getAddress();
                    log.info("Region offline: {}", address);
                    loadBalancer.removeRegion(address);
                    // 从路由表中移除该 Region 的表
                    removeRegionFromRouting(address);
                }
            });
            
            // 启动 ServiceDiscovery（加载已有节点 + 开始监听变化）
            serviceDiscovery.start();
            
            // 将已存在的 Region 加载到 LoadBalancer（兜底，防止 CuratorCache 事件遗漏）
            for (ZkConfig.RegionData region : serviceDiscovery.getOnlineRegions()) {
                String regionId = region.getHost() + ":" + region.getPort();
                String address = region.getAddress();
                loadBalancer.addRegion(regionId, address);
                log.info("Loaded existing region into LoadBalancer: {}", address);
            }
            
            log.info("ServiceDiscovery initialized, LoadBalancer synced with {} regions", 
                    serviceDiscovery.getOnlineRegions().size());
        } catch (Exception e) {
            log.error("Failed to initialize ServiceDiscovery", e);
            throw new IOException("Failed to initialize ZK discovery", e);
        }
        
        serverSocket = new ServerSocket(port);
        log.info("Master server started on port {}", port);
        
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                threadPool.submit(new MasterHandler(clientSocket, tableToRegion, loadBalancer));
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
        private final LoadBalancer loadBalancer;
        
        public MasterHandler(Socket socket, Map<String, String> tableToRegion, 
                            LoadBalancer loadBalancer) {
            this.socket = socket;
            this.tableToRegion = tableToRegion;
            this.loadBalancer = loadBalancer;
        }
        
        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            String selectedRegion = null;
            boolean success = false;
            
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
                    // 优先从路由表获取已知的表-Region映射
                    String regionAddr = tableToRegion.get(tableName);
                    
                    // 如果路由表中没有，使用 LoadBalancer 进行智能选择
                    if (regionAddr == null) {
                        regionAddr = loadBalancer.getNextRegion(tableName);
                    }
                    
                    if (regionAddr == null) {
                        sendErrorResponse(dos, msg.getRequestId(), "No available region");
                        return;
                    }
                    
                    selectedRegion = regionAddr;
                    loadBalancer.incrementConnections(selectedRegion);
                    response = Message.createResponse(
                        msg.getRequestId(),
                        NetworkConst.Status.SUCCESS,
                        regionAddr.getBytes()
                    );
                    
                } else if (requestType == NetworkConst.RequestType.CREATE_TABLE) {
                    // 使用 LoadBalancer 选择最优的 Region 来分配新表
                    String assignedRegion = loadBalancer.getNextRegion(tableName);
                    if (assignedRegion == null) {
                        sendErrorResponse(dos, msg.getRequestId(), "No available region");
                        return;
                    }
                    tableToRegion.put(tableName, assignedRegion);
                    selectedRegion = assignedRegion;
                    loadBalancer.incrementConnections(selectedRegion);
                    
                    log.info("Table '{}' assigned to region '{}' via LoadBalancer (strategy: {})",
                            tableName, assignedRegion, loadBalancer.getCurrentStrategy());
                    
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
                success = true;
                
            } catch (Exception e) {
                log.error("Error handling master request", e);
            } finally {
                // 释放连接计数并记录请求统计信息
                if (selectedRegion != null) {
                    loadBalancer.decrementConnections(selectedRegion);
                    long elapsed = System.currentTimeMillis() - startTime;
                    loadBalancer.recordRequest(selectedRegion, elapsed, success);
                }
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
    
    public LoadBalancer getLoadBalancer() {
        return loadBalancer;
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