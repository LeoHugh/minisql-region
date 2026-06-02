package com.yourname.minisql.region.ha.masterengine;

import com.yourname.minisql.region.loadbalance.LoadBalancer;
import com.yourname.minisql.region.loadbalance.RegionGroup;
import com.yourname.minisql.region.loadbalance.RegionNode;
import com.yourname.minisql.region.network.NetworkConst;
import com.yourname.minisql.region.network.protocol.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.*;

/**
 * Master 网络通信入口（Facade）
 *
 * 仅负责 Socket 监听和请求分发，业务逻辑委托给：
 *   - MetadataManager  : table → group 映射
 *   - TopologyManager  : 集群拓扑 & 故障检测
 *   - LoadBalancer      : 策略选择（无状态）
 */
public class MasterServer {
    private static final Logger log = LoggerFactory.getLogger(MasterServer.class);
    
    private final int port;
    private ServerSocket serverSocket;
    private final ExecutorService threadPool;
    private volatile boolean running = true;
    
    // 三个核心 Manager
    private final MetadataManager metadataManager;
    private final TopologyManager topologyManager;
    private final LoadBalancer loadBalancer;
    
    /**
     * 完整构造器：由 HAMasterServer 或外部注入已初始化的 Manager
     */
    public MasterServer(int port, MetadataManager metadataManager,
                        TopologyManager topologyManager, LoadBalancer loadBalancer) {
        this.port = port;
        this.metadataManager = metadataManager;
        this.topologyManager = topologyManager;
        this.loadBalancer = loadBalancer;
        this.threadPool = Executors.newCachedThreadPool();
    }
    
    /**
     * 简单构造器（兼容旧测试代码）：内部创建默认 Manager 并在 start() 中自动启动
     */
    public MasterServer(int port) {
        this(port, new MetadataManager(), new TopologyManager(), new LoadBalancer());
    }
    
    public void start() throws IOException {
        // 如果是简单构造，需要自动启动 Manager
        try {
            metadataManager.start();
            topologyManager.start();
            log.info("Managers initialized (MetadataManager + TopologyManager)");
        } catch (Exception e) {
            log.error("Failed to initialize managers", e);
            throw new IOException("Failed to initialize managers", e);
        }
        
        serverSocket = new ServerSocket(port);
        log.info("Master server started on port {}", port);
        
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                threadPool.submit(new MasterHandler(clientSocket,
                        metadataManager, topologyManager, loadBalancer));
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
    }
    
    /**
     * MasterHandler —— 处理 Client 请求，通过三个 Manager 协作完成
     */
    private static class MasterHandler implements Runnable {
        private final Socket socket;
        private final MetadataManager metadataManager;
        private final TopologyManager topologyManager;
        private final LoadBalancer loadBalancer;
        
        public MasterHandler(Socket socket, MetadataManager metadataManager,
                             TopologyManager topologyManager, LoadBalancer loadBalancer) {
            this.socket = socket;
            this.metadataManager = metadataManager;
            this.topologyManager = topologyManager;
            this.loadBalancer = loadBalancer;
        }
        
        @Override
        public void run() {
            String selectedRegion = null;
            
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
                    // 通过 MetadataManager 查映射 + TopologyManager 查拓扑
                    String groupInfo = loadBalancer.getGroupInfoForTable(
                            tableName, metadataManager.getTableToGroup(), topologyManager.getGroupMap());
                    
                    if (groupInfo == null) {
                        sendErrorResponse(dos, msg.getRequestId(), "No available region group for table");
                        return;
                    }
                    
                    // 获取 Master 地址用于连接计数
                    String groupId = metadataManager.getGroupForTable(tableName);
                    if (groupId != null) {
                        RegionGroup group = topologyManager.getGroup(groupId);
                        if (group != null && group.getMaster() != null) {
                            selectedRegion = group.getMaster().getAddress();
                        }
                    }
                    
                    if (selectedRegion != null) {
                        topologyManager.incrementConnections(selectedRegion);
                    }
                    
                    response = Message.createResponse(
                        msg.getRequestId(),
                        NetworkConst.Status.SUCCESS,
                        groupInfo.getBytes()
                    );
                    
                } else if (requestType == NetworkConst.RequestType.CREATE_TABLE) {
                    // 通过 LoadBalancer 选择 Group（无状态）
                    String groupId = loadBalancer.selectGroup(
                            topologyManager.getGroupMap(), tableName);
                    if (groupId == null) {
                        sendErrorResponse(dos, msg.getRequestId(), "No available region group");
                        return;
                    }
                    
                    // 通过 MetadataManager 登记映射（持久化到 ZK）
                    try {
                        metadataManager.assignTableToGroup(tableName, groupId);
                    } catch (Exception e) {
                        log.error("Failed to persist table mapping", e);
                        sendErrorResponse(dos, msg.getRequestId(), "Failed to persist table mapping");
                        return;
                    }
                    
                    // 从 TopologyManager 获取该 Group 的 Master 地址
                    String masterAddr = null;
                    RegionGroup group = topologyManager.getGroup(groupId);
                    if (group != null && group.getMaster() != null) {
                        masterAddr = group.getMaster().getAddress();
                    }
                    
                    if (masterAddr == null) {
                        sendErrorResponse(dos, msg.getRequestId(), "No master in selected group: " + groupId);
                        return;
                    }
                    
                    selectedRegion = masterAddr;
                    topologyManager.incrementConnections(selectedRegion);
                    
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
                
            } catch (Exception e) {
                log.error("Error handling master request", e);
            } finally {
                // 释放连接计数
                if (selectedRegion != null) {
                    topologyManager.decrementConnections(selectedRegion);
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
    
    public MetadataManager getMetadataManager() {
        return metadataManager;
    }
    
    public TopologyManager getTopologyManager() {
        return topologyManager;
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