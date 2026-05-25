package com.yourname.minisql.region.client;

import com.yourname.minisql.region.network.NetworkConst;
import com.yourname.minisql.region.network.protocol.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class Client {
    private static final Logger log = LoggerFactory.getLogger(Client.class);
    
    private final String masterHost;
    private final int masterPort;
    private final AtomicLong requestIdGen = new AtomicLong(1);
    
    // 缓存表名到 Region 的映射
    private final Map<String, String> regionCache = new ConcurrentHashMap<>();
    
    // 配置参数
    private int maxRetries = 3;           // 最大重试次数
    private long retryIntervalMs = 1000;  // 重试间隔（毫秒）
    private boolean enableCache = true;   // 是否启用缓存
    private long cacheTtlMs = 60000;      // 缓存过期时间（毫秒）
    private final Map<String, Long> cacheTimestamp = new ConcurrentHashMap<>();
    
    public Client(String masterHost, int masterPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }
    
    public Client(String masterHost, int masterPort, int maxRetries) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.maxRetries = maxRetries;
    }
    
    /**
     * 获取 Active Master 地址
     */
    private String[] getActiveMaster() {
        try {
            com.yourname.minisql.region.zk.ZkClientManager zk = com.yourname.minisql.region.zk.ZkClientManager.getInstance();
            if (!zk.isInitialized()) {
                zk.init();
            }
            byte[] data = zk.getNodeData(com.yourname.minisql.region.zk.ZkConfig.ZK_MASTER_PATH + "/active");
            if (data != null && data.length > 0) {
                String addr = new String(data);
                String[] parts = addr.split(":");
                if (parts.length == 2) {
                    return parts;
                }
            }
        } catch (Exception e) {
            log.warn("Failed to get active master from ZK, falling back to static config: {}", e.getMessage());
        }
        return new String[]{masterHost, String.valueOf(masterPort)};
    }
    
    /**
     * 向 Master 发送请求，获取 Region 地址
     */
    private String sendToMaster(int requestType, String tableName) throws IOException {
        String body = requestType + " " + tableName;
        Message request = Message.createRequest(
            requestIdGen.getAndIncrement(),
            NetworkConst.MessageType.REQUEST,
            body.getBytes()
        );
        
        String[] masterInfo = getActiveMaster();
        String currentHost = masterInfo[0];
        int currentPort = Integer.parseInt(masterInfo[1]);
        
        try (Socket socket = new Socket(currentHost, currentPort);
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
            return response.getBodyAsString();
        }
    }
    
    /**
     * 向 Region 发送 SQL 请求（支持超时和重试）
     */
    private String sendToRegion(String regionAddr, String sql) throws IOException {
        String[] parts = regionAddr.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        
        Message request = Message.createRequest(
            requestIdGen.getAndIncrement(),
            NetworkConst.MessageType.REQUEST,
            sql.getBytes()
        );
        
        // 设置连接超时
        try (Socket socket = new Socket()) {
            socket.connect(new java.net.InetSocketAddress(host, port), 5000);
            socket.setSoTimeout(10000);  // 读取超时
            
            try (DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                 DataInputStream dis = new DataInputStream(socket.getInputStream())) {
                
                byte[] reqData = request.encode();
                dos.writeInt(reqData.length);
                dos.write(reqData);
                dos.flush();
                
                int len = dis.readInt();
                byte[] respData = new byte[len];
                dis.readFully(respData);
                
                Message response = Message.decode(respData);
                return response.getBodyAsString();
            }
        }
    }
    /**
     * 暴露对外的接口
     * @param sql SQL 语句
     * @return SQL 执行结果
     */
    public String execute(String sql) {
        return execute(sql, maxRetries);
    }

    /**
     * 带重试和故障转移的 SQL 执行
     * @param sql SQL 语句
     * @param retries 重试次数
     * @return SQL 执行结果
     */
    public String execute(String sql, int retries) {
        String tableName = extractTableName(sql);
        if (tableName == null) {
            String error = "Error: Cannot extract table name from: " + sql;
            System.out.println(error);
            return error;
        }
    
        boolean isCreateTable = sql.trim().toUpperCase().startsWith("CREATE");
    
        for (int attempt = 0; attempt <= retries; attempt++) {
        try {
            // 获取 Region 地址（支持缓存）
            String regionAddr = getRegionAddress(tableName, isCreateTable, attempt);
            System.out.println("Got region address: " + regionAddr);
            
            // 执行 SQL
            long startTime = System.currentTimeMillis();
            String result = sendToRegion(regionAddr, sql);
            long responseTime = System.currentTimeMillis() - startTime;
            
            System.out.println("Result: " + result);
            
            // 记录成功（用于负载均衡统计）
            recordSuccess(regionAddr, responseTime);
            
            return result;  // 返回结果
            
        } catch (Exception e) {
            System.out.println("Attempt " + (attempt + 1) + " failed: " + e.getMessage());
            log.warn("SQL execution failed (attempt {}/{}): {}", attempt + 1, retries + 1, e.getMessage());
            
            // 清除缓存，下次重新获取
            regionCache.remove(tableName);
            cacheTimestamp.remove(tableName);
            
            if (attempt == retries) {
                String error = "Error: Failed after " + (retries + 1) + " attempts: " + e.getMessage();
                System.out.println(error);
                return error;
            } else {
                // 等待后重试
                try {
                    Thread.sleep(retryIntervalMs * (attempt + 1));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return "Error: Interrupted";
                }
            }
        }
    }
    
    return "Error: Unexpected end of execution";
    }

    
    /**
     * 获取 Region 地址（支持缓存和负载均衡）
     */
    private String getRegionAddress(String tableName, boolean isCreateTable, int attempt) throws IOException {
        // 建表请求总是从 Master 获取（用于创建路由）
        if (isCreateTable) {
            return sendToMaster(NetworkConst.RequestType.CREATE_TABLE, tableName);
        }
        
        // 检查缓存
        if (enableCache) {
            String cached = regionCache.get(tableName);
            Long timestamp = cacheTimestamp.get(tableName);
            
            if (cached != null && timestamp != null && 
                (System.currentTimeMillis() - timestamp) < cacheTtlMs) {
                log.debug("Using cached region for {}: {}", tableName, cached);
                return cached;
            }
        }
        
        // 从 Master 获取（Master 会做负载均衡）
        String regionAddr = sendToMaster(NetworkConst.RequestType.GET_REGION, tableName);
        
        // 更新缓存
        if (enableCache) {
            regionCache.put(tableName, regionAddr);
            cacheTimestamp.put(tableName, System.currentTimeMillis());
        }
        
        return regionAddr;
    }
    
    /**
     * 记录成功（用于 Master 端的负载均衡统计）
     * 可以通过额外的 API 上报，这里简化处理
     */
    private void recordSuccess(String regionAddr, long responseTime) {
        log.debug("Request to {} succeeded in {}ms", regionAddr, responseTime);
        // TODO: 可以上报到 Master 用于负载均衡决策
    }
    
    /**
     * 刷新缓存
     */
    public void refreshCache() {
        regionCache.clear();
        cacheTimestamp.clear();
        System.out.println("Cache refreshed");
    }
    
    /**
     * 设置最大重试次数
     */
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }
    
    /**
     * 设置重试间隔
     */
    public void setRetryIntervalMs(long intervalMs) {
        this.retryIntervalMs = intervalMs;
    }
    
    /**
     * 启用/禁用缓存
     */
    public void setEnableCache(boolean enable) {
        this.enableCache = enable;
        if (!enable) {
            refreshCache();
        }
    }
    
    /**
     * 提取表名
     */
    private String extractTableName(String sql) {
        String upper = sql.trim().toUpperCase();
        
        if (upper.startsWith("CREATE TABLE")) {
            // CREATE TABLE users (...) -> users
            String[] parts = sql.trim().split("\\s+");
            String tableName = parts[2];
            // 去掉可能存在的括号
            int parenIndex = tableName.indexOf('(');
            if (parenIndex > 0) {
                tableName = tableName.substring(0, parenIndex);
            }
            return tableName;
            
        } else if (upper.startsWith("INSERT INTO")) {
            // INSERT INTO users ... -> users
            String[] parts = sql.trim().split("\\s+");
            return parts[2];
            
        } else if (upper.startsWith("SELECT")) {
            // SELECT * FROM users ... -> users
            int fromIdx = upper.indexOf("FROM");
            if (fromIdx > 0) {
                String afterFrom = sql.substring(fromIdx + 4).trim();
                String[] parts = afterFrom.split("\\s+");
                String tableName = parts[0];
                // 去掉可能存在的逗号或换行
                if (tableName.endsWith(",")) {
                    tableName = tableName.substring(0, tableName.length() - 1);
                }
                return tableName;
            }
            
        } else if (upper.startsWith("DELETE FROM")) {
            String[] parts = sql.trim().split("\\s+");
            return parts[2];
            
        } else if (upper.startsWith("UPDATE")) {
            // UPDATE users SET ... -> users
            String[] parts = sql.trim().split("\\s+");
            return parts[1];
        }
        
        return null;
    }
    
    /**
     * 健康检查 - 测试 Master 连接
     */
    public boolean healthCheck() {
        try {
            sendToMaster(NetworkConst.RequestType.GET_REGION, "health_check");
            return true;
        } catch (Exception e) {
            log.warn("Health check failed: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * 获取统计信息
     */
    public String getStats() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n=== Client Stats ===\n");
        sb.append("Master: ").append(masterHost).append(":").append(masterPort).append("\n");
        sb.append("Cached tables: ").append(regionCache.size()).append("\n");
        sb.append("Cache enabled: ").append(enableCache).append("\n");
        sb.append("Max retries: ").append(maxRetries).append("\n");
        sb.append("Cache TTL: ").append(cacheTtlMs).append("ms\n");
        
        if (!regionCache.isEmpty()) {
            sb.append("Cached routes:\n");
            for (Map.Entry<String, String> entry : regionCache.entrySet()) {
                sb.append("  ").append(entry.getKey()).append(" -> ").append(entry.getValue()).append("\n");
            }
        }
        
        return sb.toString();
    }
    
    public static void main(String[] args) {
        // 支持命令行参数：host, port, retries
        String host = "localhost";
        int port = NetworkConst.MASTER_PORT;
        int retries = 3;
        
        if (args.length > 0) {
            host = args[0];
        }
        if (args.length > 1) {
            port = Integer.parseInt(args[1]);
        }
        if (args.length > 2) {
            retries = Integer.parseInt(args[2]);
        }
        
        Client client = new Client(host, port, retries);
        Scanner scanner = new Scanner(System.in);
        
        System.out.println("=== MiniSQL Client (HA Version) ===");
        System.out.println("Connected to master at " + host + ":" + port);
        System.out.println("Max retries: " + retries);
        System.out.println("\nSpecial commands:");
        System.out.println("  stats    - show client statistics");
        System.out.println("  refresh  - refresh region cache");
        System.out.println("  exit     - quit");
        System.out.println();
        
        while (true) {
            System.out.print("sql> ");
            String line = scanner.nextLine().trim();
            
            if (line.equalsIgnoreCase("exit")) {
                break;
            }
            
            if (line.equalsIgnoreCase("stats")) {
                System.out.println(client.getStats());
                continue;
            }
            
            if (line.equalsIgnoreCase("refresh")) {
                client.refreshCache();
                System.out.println("Cache refreshed");
                continue;
            }
            
            if (line.isEmpty()) {
                continue;
            }
            
            client.execute(line);
        }
        
        System.out.println("Goodbye!");
        scanner.close();
    }
}