package com.yourname.minisql.region.replication;

import com.yourname.minisql.region.manager.DatabaseManager;
import com.yourname.minisql.region.model.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 主从复制管理器
 */
public class ReplicationManager {
    private static final Logger log = LoggerFactory.getLogger(ReplicationManager.class);
    
    private final DatabaseManager dbManager;
    private final String regionId;
    private String role = "STANDBY";
    private String masterAddress;
    private final java.util.List<SlaveConnection> slaves = new java.util.concurrent.CopyOnWriteArrayList<>();
    
    private ServerSocket replicationServer;
    private ExecutorService replicationExecutor;
    private ScheduledExecutorService heartbeatExecutor;
    private volatile boolean running = true;
    private int replicationPort = 0;
    
    public ReplicationManager(DatabaseManager dbManager, String regionId) {
        this.dbManager = dbManager;
        this.regionId = regionId;
        this.replicationExecutor = Executors.newCachedThreadPool();
        this.heartbeatExecutor = Executors.newScheduledThreadPool(2);
    }
    
    /**
     * 设置为 Master 模式
     */
    public void becomeMaster() {
        this.role = "MASTER";
        log.info("=== Region {} 成为 MASTER ===", regionId);
        startReplicationServer();
    }
    
    /**
     * 设置为 Slave 模式
     */
    public void becomeSlave(String masterAddr) {
        this.role = "SLAVE";
        this.masterAddress = masterAddr;
        log.info("=== Region {} 成为 SLAVE，主节点: {} ===", regionId, masterAddr);
        startSlaveSync();
    }
    
    /**
     * 启动复制服务（Master 端）
     */
    private void startReplicationServer() {
        try {
            replicationServer = new ServerSocket(0);
            replicationPort = replicationServer.getLocalPort();
            log.info("Master 复制服务已启动，端口: {}", replicationPort);
            
            replicationExecutor.submit(() -> {
                while (running) {
                    try {
                        Socket slaveSocket = replicationServer.accept();
                        log.info("Slave 连接成功: {}", slaveSocket.getRemoteSocketAddress());
                        
                        SlaveConnection connection = new SlaveConnection(slaveSocket, this);
                        slaves.add(connection);
                        replicationExecutor.submit(connection);
                    } catch (IOException e) {
                        if (running) {
                            log.error("复制服务接受连接失败", e);
                        }
                    }
                }
            });
        } catch (IOException e) {
            log.error("启动复制服务失败", e);
        }
    }
    
    /**
     * 启动从节点同步（Slave 端）
     */
    private void startSlaveSync() {
        replicationExecutor.submit(() -> {
            while (running) {
                try {
                    syncWithMaster();
                    Thread.sleep(1000);
                } catch (Exception e) {
                    log.error("与主节点同步失败: {}", e.getMessage());
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        });
    }
    
    /**
     * 从主节点同步数据
     */
    private void syncWithMaster() {
        if (masterAddress == null) {
            log.warn("主节点地址未设置");
            return;
        }
        
        String[] parts = masterAddress.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        
        log.debug("尝试连接主节点 {}:{}", host, port);
        
        try (Socket socket = new Socket(host, port);
             DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
             DataInputStream dis = new DataInputStream(socket.getInputStream())) {
            
            log.info("已连接到主节点 {}:{}", host, port);
            
            dos.writeByte(ReplicationProtocol.Command.DATA_SYNC);
            dos.writeLong(0);
            dos.flush();
            
            while (running && !socket.isClosed()) {
                byte response;
                try {
                    response = dis.readByte();
                } catch (EOFException e) {
                    break;
                }
                
                if (response == 0) {
                    int entryCount = dis.readInt();
                    log.info("收到 {} 条日志条目", entryCount);
                    for (int i = 0; i < entryCount; i++) {
                        receiveAndApplyEntry(dis);
                    }
                } else if (response == ReplicationProtocol.Command.REPLICATE_SQL) {
                    String sql = dis.readUTF();
                    log.info("收到复制 SQL: {}", sql);
                    dbManager.execute(sql);
                }
            }
            
        } catch (IOException e) {
            log.error("同步失败: {}", e.getMessage());
        }
    }
    
    /**
     * 接收并应用日志条目
     */
    private void receiveAndApplyEntry(DataInputStream dis) throws IOException {
        byte opCode = dis.readByte();
        int keyLen = dis.readInt();
        byte[] key = new byte[keyLen];
        dis.readFully(key);
        
        log.debug("收到操作: {}, key: {}", opCode == 0 ? "PUT" : "DELETE", new String(key));
        
        if (opCode == 0) {
            int rowLen = dis.readInt();
            byte[] rowBytes = new byte[rowLen];
            dis.readFully(rowBytes);
            Row row = Row.fromBytes(rowBytes);
            dbManager.getStorage().put(key, row);
            log.debug("已应用 PUT 操作");
        } else {
            dbManager.getStorage().delete(key);
            log.debug("已应用 DELETE 操作");
        }
    }
    
    /**
     * Master 端：将 SQL 推送到所有从节点
     */
    public void replicateSQL(String sql) {
        if (!"MASTER".equals(role)) {
            return;
        }
        
        log.debug("复制 SQL 到 {} 个从节点: {}", slaves.size(), sql);
        
        for (SlaveConnection slave : slaves) {
            slave.sendSQL(sql);
        }
    }
    
    /**
     * 获取复制端口（用于从节点连接）
     */
    public int getReplicationPort() {
        return replicationPort;
    }
    
    /**
     * 获取角色
     */
    public String getRole() {
        return role;
    }
    
    /**
     * 关闭
     */
    public void close() {
        running = false;
        try {
            if (replicationServer != null) {
                replicationServer.close();
            }
        } catch (IOException e) {
            log.error("关闭复制服务失败", e);
        }
        replicationExecutor.shutdown();
        heartbeatExecutor.shutdown();
        for (SlaveConnection slave : slaves) {
            slave.close();
        }
        log.info("ReplicationManager 已关闭");
    }
    
    /**
     * 从节点连接处理类
     */
    private static class SlaveConnection implements Runnable {
        private final Socket socket;
        private final ReplicationManager manager;
        private DataInputStream dis;
        private DataOutputStream dos;
        private volatile boolean connected = true;
        
        public SlaveConnection(Socket socket, ReplicationManager manager) {
            this.socket = socket;
            this.manager = manager;
        }
        
        @Override
        public void run() {
            log.info("SlaveConnection 启动");
            
            try {
                dis = new DataInputStream(socket.getInputStream());
                dos = new DataOutputStream(socket.getOutputStream());
                
                while (connected && !socket.isClosed()) {
                    try {
                        byte command = dis.readByte();
                        
                        switch (command) {
                            case ReplicationProtocol.Command.DATA_SYNC:
                                handleDataSync();
                                break;
                            case ReplicationProtocol.Command.HEARTBEAT:
                                dos.writeByte(0);
                                dos.flush();
                                break;
                            default:
                                log.warn("未知命令: {}", command);
                        }
                    } catch (EOFException e) {
                        break;
                    }
                }
            } catch (IOException e) {
                log.error("SlaveConnection 错误", e);
            } finally {
                close();
                manager.slaves.remove(this);
            }
        }
        
        private void handleDataSync() throws IOException {
            long fromPosition = dis.readLong();
            log.debug("从节点请求同步，位置: {}", fromPosition);
            
            // 返回空列表（简化实现）
            dos.writeByte(0);
            dos.writeInt(0);
            dos.flush();
        }
        
        public void sendSQL(String sql) {
            if (!connected) return;
            
            try {
                dos.writeByte(ReplicationProtocol.Command.REPLICATE_SQL);
                dos.writeUTF(sql);
                dos.flush();
            } catch (IOException e) {
                log.error("发送 SQL 失败", e);
                connected = false;
            }
        }
        
        public void close() {
            connected = false;
            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
            } catch (IOException e) {
                log.error("关闭连接失败", e);
            }
        }
    }
}