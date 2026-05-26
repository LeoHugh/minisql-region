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
    
    // 负载均衡器（Group-based）
    private final LoadBalancer loadBalancer;
    
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
            
            // 注册内部监听器（LoadBalancer 同步 —— 按 Group 分组）
            serviceDiscovery.addListener(new ServiceDiscovery.RegionChangeListener() {
                @Override
                public void onRegionOnline(ZkConfig.RegionData region) {
                    String address = region.getAddress();
                    String regionId = region.getHost() + ":" + region.getPort();
                    String groupId = region.getGroupId();
                    String role = region.getRole();
                    
                    if (groupId == null || groupId.isEmpty()) {
                        groupId = "default";
                    }
                    if (role == null || role.isEmpty()) {
                        role = "STANDBY";
                    }
                    
                    log.info("Region online: group='{}', role='{}', addr='{}'", groupId, role, address);
                    loadBalancer.addRegionToGroup(groupId, role, regionId, address);
                    loadBalancer.markAvailable(address, true);
                }
                
                @Override
                public void onRegionOffline(ZkConfig.RegionData region) {
                    String address = region.getAddress();
                    String groupId = region.getGroupId();
                    
                    if (groupId != null && !groupId.isEmpty()) {
                        log.info("Region offline: group='{}', addr='{}'", groupId, address);
                        loadBalancer.removeRegionFromGroup(groupId, address);
                    } else {
                        log.info("Region offline: addr='{}' (no groupId, removing globally)", address);
                        loadBalancer.removeRegion(address);
                    }
                    
                    // 清理路由表
                    loadBalancer.removeRegionFromRouting(address);
                }
            });
            
            // 启动 ServiceDiscovery（加载已有节点 + 开始监听变化）
            serviceDiscovery.start();
            
            // 将已存在的 Region 加载到 LoadBalancer（兜底，防止 CuratorCache 事件遗漏）
            for (ZkConfig.RegionData region : serviceDiscovery.getOnlineRegions()) {
                String regionId = region.getHost() + ":" + region.getPort();
                String address = region.getAddress();
                String groupId = region.getGroupId();
                String role = region.getRole();
                
                if (groupId == null || groupId.isEmpty()) groupId = "default";
                if (role == null || role.isEmpty()) role = "STANDBY";
                
                loadBalancer.addRegionToGroup(groupId, role, regionId, address);
                log.info("Loaded existing region into LoadBalancer: group='{}', role='{}', addr='{}'",
                        groupId, role, address);
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
                threadPool.submit(new MasterHandler(clientSocket, loadBalancer));
            } catch (IOException e) {
                if (running) {
                    log.error("Error accepting connection", e);
                }
            }
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
    
    /**
     * MasterHandler —— 处理 Client 请求，使用 Group-based LoadBalancer
     */
    private static class MasterHandler implements Runnable {
        private final Socket socket;
        private final LoadBalancer loadBalancer;
        
        public MasterHandler(Socket socket, LoadBalancer loadBalancer) {
            this.socket = socket;
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
                    // 优先查找已有的 表->Group 映射
                    String groupInfo = loadBalancer.getGroupInfoForTable(tableName);
                    
                    if (groupInfo == null) {
                        // 没有映射，兼容旧行为：返回一个 Master 地址
                        String regionAddr = loadBalancer.getNextRegion(tableName);
                        if (regionAddr == null) {
                            sendErrorResponse(dos, msg.getRequestId(), "No available region");
                            return;
                        }
                        selectedRegion = regionAddr;
                        // 返回单地址格式（向后兼容）
                        groupInfo = regionAddr;
                    } else {
                        // 从 groupInfo 中提取 master 地址用于统计
                        String masterAddr = loadBalancer.getMasterAddressForTable(tableName);
                        if (masterAddr != null) {
                            selectedRegion = masterAddr;
                        }
                    }
                    
                    loadBalancer.incrementConnections(selectedRegion != null ? selectedRegion : "");
                    response = Message.createResponse(
                        msg.getRequestId(),
                        NetworkConst.Status.SUCCESS,
                        groupInfo.getBytes()
                    );
                    
                } else if (requestType == NetworkConst.RequestType.CREATE_TABLE) {
                    // 使用 LoadBalancer 选择一个 RegionGroup
                    String groupId = loadBalancer.selectGroup(tableName);
                    if (groupId == null) {
                        sendErrorResponse(dos, msg.getRequestId(), "No available region group");
                        return;
                    }
                    
                    // 登记 表 -> Group 映射
                    loadBalancer.assignTableToGroup(tableName, groupId);
                    
                    // 获取该 Group 的 Master 地址（建表需要路由到 Master）
                    String masterAddr = loadBalancer.getMasterAddressForTable(tableName);
                    if (masterAddr == null) {
                        sendErrorResponse(dos, msg.getRequestId(), "No master in selected group: " + groupId);
                        return;
                    }
                    
                    selectedRegion = masterAddr;
                    loadBalancer.incrementConnections(selectedRegion);
                    
                    log.info("Table '{}' assigned to group '{}' (master={}), strategy={}",
                            tableName, groupId, masterAddr, loadBalancer.getCurrentStrategy());
                    
                    // 返回 Master 地址（建表操作只走 Master）
                    response = Message.createResponse(
                        msg.getRequestId(),
                        NetworkConst.Status.SUCCESS,
                        masterAddr.getBytes()
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