package com.yourname.minisql.region.network;

import com.yourname.minisql.region.manager.DatabaseManager;
import com.yourname.minisql.region.network.protocol.Message;
import com.yourname.minisql.region.replication.ReplicationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RegionServer {
    private static final Logger log = LoggerFactory.getLogger(RegionServer.class);
    private final int port;
    private final DatabaseManager dbManager;
    private final ReplicationManager replicationManager;
    private ServerSocket serverSocket;
    private final ExecutorService threadPool;
    private volatile boolean running = true;
    
    /**
     * 构造方法（无复制管理器，兼容旧用法）
     */
    public RegionServer(int port, DatabaseManager dbManager) {
        this(port, dbManager, null);
    }
    
    /**
     * 构造方法（带复制管理器，支持 Slave 只读保护）
     */
    public RegionServer(int port, DatabaseManager dbManager, ReplicationManager replicationManager) {
        this.port = port;
        this.dbManager = dbManager;
        this.replicationManager = replicationManager;
        this.threadPool = Executors.newCachedThreadPool();
    }
    
    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        log.info("Region server started on port {}", port);
        
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                threadPool.submit(new ClientHandler(clientSocket, dbManager, replicationManager));
            } catch (IOException e) {
                if (running) {
                    log.error("Error accepting connection", e);
                }
            }
        }
    }
    
    public void stop() throws IOException {
        running = false;
        if (serverSocket != null) {
            serverSocket.close();
        }
        threadPool.shutdown();
    }
    
    private static class ClientHandler implements Runnable {
        private final Socket socket;
        private final DatabaseManager dbManager;
        private final ReplicationManager replicationManager;
        
        public ClientHandler(Socket socket, DatabaseManager dbManager, ReplicationManager replicationManager) {
            this.socket = socket;
            this.dbManager = dbManager;
            this.replicationManager = replicationManager;
        }
        
        @Override
        public void run() {
            try (DataInputStream dis = new DataInputStream(socket.getInputStream());
                 DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {
                
                log.info("Client connected: {}", socket.getRemoteSocketAddress());
                
                // 读取消息长度（简化版：先读4字节长度，再读消息体）
                while (true) {
                    int length;
                    try {
                        length = dis.readInt();
                    } catch (EOFException e) {
                        break;
                    }
                    
                    byte[] data = new byte[length];
                    dis.readFully(data);
                    
                    Message msg = Message.decode(data);
                    log.debug("Received message: type={}, requestId={}", msg.getType(), msg.getRequestId());
                    
                    // 处理 SQL 请求
                    if (msg.getType() == NetworkConst.MessageType.REQUEST) {
                        String sql = msg.getBodyAsString();
                        log.info("Executing SQL: {}", sql);
                        
                        // Slave 只读保护：拒绝写操作
                        if (isSlaveMode() && isWriteOperation(sql)) {
                            String errorMsg = "Error: Current node is SLAVE (read-only). Write operations are not allowed. " +
                                              "Please send writes to the MASTER node.";
                            log.warn("Rejected write operation on SLAVE: {}", sql);
                            
                            Message response = Message.createResponse(
                                msg.getRequestId(),
                                NetworkConst.Status.ERROR,
                                errorMsg.getBytes()
                            );
                            dos.writeInt(response.encode().length);
                            dos.write(response.encode());
                            dos.flush();
                            continue;
                        }
                        
                        String result = dbManager.execute(sql);
                        
                        Message response = Message.createResponse(
                            msg.getRequestId(),
                            NetworkConst.Status.SUCCESS,
                            result.getBytes()
                        );
                        dos.writeInt(response.encode().length);
                        dos.write(response.encode());
                        dos.flush();
                    }
                }
            } catch (IOException e) {
                log.error("Error handling client", e);
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    log.error("Error closing socket", e);
                }
            }
        }
        
        /**
         * 判断当前节点是否为 Slave 模式
         */
        private boolean isSlaveMode() {
            return replicationManager != null && "SLAVE".equals(replicationManager.getRole());
        }
        
        /**
         * 判断 SQL 是否为写操作
         */
        private boolean isWriteOperation(String sql) {
            if (sql == null || sql.isEmpty()) {
                return false;
            }
            String sqlUpper = sql.trim().toUpperCase();
            return sqlUpper.startsWith("INSERT") 
                || sqlUpper.startsWith("UPDATE") 
                || sqlUpper.startsWith("DELETE") 
                || sqlUpper.startsWith("CREATE") 
                || sqlUpper.startsWith("DROP") 
                || sqlUpper.startsWith("ALTER");
        }
    }
}