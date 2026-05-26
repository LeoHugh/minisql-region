package com.yourname.minisql.region.zk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.fastjson2.JSON;

public class ZkConfig {
    private static final Logger log = LoggerFactory.getLogger(ZkConfig.class);
    
    // Zookeeper 连接地址（单机版，集群用逗号分隔）
    public static final String ZK_CONNECT_STRING = "localhost:2181";
    
    // ZK 中项目根路径
    public static final String ZK_BASE_PATH = "/minisql";
    
    // Region 注册的父路径（旧的全局路径，保留兼容）
    public static final String ZK_REGIONS_PATH = ZK_BASE_PATH + "/regions";
    
    // Master 注册的路径
    public static final String ZK_MASTER_PATH = ZK_BASE_PATH + "/master";
    
    // 分组（Replica Group / Shard）根路径
    public static final String ZK_GROUPS_PATH = ZK_BASE_PATH + "/groups";
    
    /**
     * 获取指定 groupId 的 regions 路径: /minisql/groups/<groupId>/regions
     */
    public static String getGroupRegionsPath(String groupId) {
        if (TestConfig.isTestMode) {
            return TestConfig.TEST_ZK_BASE_PATH + "/groups/" + groupId + "/regions";
        }
        return ZK_GROUPS_PATH + "/" + groupId + "/regions";
    }
    
    // 会话超时时间（毫秒）
    public static final int SESSION_TIMEOUT_MS = 30000;
    
    // 连接超时时间（毫秒）
    public static final int CONNECTION_TIMEOUT_MS = 15000;
    
    // 重试间隔（毫秒）
    public static final int RETRY_INTERVAL_MS = 1000;
    
    // 最大重试次数
    public static final int MAX_RETRIES = 3;
    
    /**
     * 获取当前使用的 Region 路径（根据测试模式自动切换）
     */
    public static String getRegionsPath() {
        if (TestConfig.isTestMode) {
            return TestConfig.TEST_ZK_REGIONS_PATH;
        }
        return ZK_REGIONS_PATH;
    }
    
    /**
     * 获取当前使用的 Master 路径
     */
    public static String getMasterPath() {
        if (TestConfig.isTestMode) {
            return TestConfig.TEST_ZK_MASTER_PATH;
        }
        return ZK_MASTER_PATH;
    }
    
    /**
     * 获取当前使用的 Base 路径
     */
    public static String getBasePath() {
        if (TestConfig.isTestMode) {
            return TestConfig.TEST_ZK_BASE_PATH;
        }
        return ZK_BASE_PATH;
    }
    
    // 测试环境专用配置
    public static class TestConfig {
        public static final String TEST_ZK_CONNECT_STRING = "localhost:2181";
        public static final String TEST_ZK_BASE_PATH = "/minisql_test";
        public static final String TEST_ZK_REGIONS_PATH = TEST_ZK_BASE_PATH + "/regions";
        public static final String TEST_ZK_MASTER_PATH = TEST_ZK_BASE_PATH + "/master";
        public static final String TEST_ZK_GROUPS_PATH = TEST_ZK_BASE_PATH + "/groups";
        public static final int TEST_SESSION_TIMEOUT_MS = 5000;
        public static final int TEST_CONNECTION_TIMEOUT_MS = 3000;
        
        // 测试专用标志，用于隔离测试数据
        public static volatile boolean isTestMode = false;
        
        // 测试端口范围（避免冲突）
        public static int getTestMasterPort(int index) {
            return 19000 + index;
        }
        
        public static int getTestRegionPort(int index) {
            return 18800 + index;
        }
    }
    
    // 节点数据编码（JSON 格式）
    public static class RegionData {
        private String host;
        private int port;
        private String status;
        private long timestamp;
        private String role;            // "MASTER" / "SLAVE" / "STANDBY"
        private int replicationPort;    // Master 的复制端口（仅 Master 有效）
        private String groupId;         // 所属分组 ID（Replica Group / Shard）
        
        public RegionData() {}
        
        public RegionData(String host, int port, String status) {
            this.host = host;
            this.port = port;
            this.status = status;
            this.timestamp = System.currentTimeMillis();
        }
        
        // Getters and Setters
        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
        public String getRole() { return role; }
        public void setRole(String role) { this.role = role; }
        public int getReplicationPort() { return replicationPort; }
        public void setReplicationPort(int replicationPort) { this.replicationPort = replicationPort; }
        public String getGroupId() { return groupId; }
        public void setGroupId(String groupId) { this.groupId = groupId; }
        
        public String getAddress() {
            return host + ":" + port;
        }
        
        /**
         * 将 RegionData 转换为 JSON 字节数组
         */
        public byte[] toBytes() {
            return JSON.toJSONBytes(this);
        }
        
        /**
         * 从字节数组解析 RegionData（支持 JSON 和纯地址格式）
         * @param data 字节数组
         * @return RegionData 对象，解析失败返回 null
         */
        public static RegionData fromBytes(byte[] data) {
            if (data == null || data.length == 0) {
                log.debug("Cannot parse null or empty data");
                return null;
            }
            
            String str = new String(data);
            log.debug("Parsing region data: {}", str);
            
            // 尝试解析 JSON 格式
            if (str.trim().startsWith("{")) {
                try {
                    RegionData region = JSON.parseObject(str, RegionData.class);
                    log.debug("Parsed JSON region data: {}", region);
                    return region;
                } catch (Exception e) {
                    log.warn("Failed to parse JSON region data: {}", str, e);
                }
            }
            
            // 兼容旧格式：纯地址字符串 "host:port"
            if (str.contains(":")) {
                String[] parts = str.split(":");
                if (parts.length >= 2) {
                    try {
                        RegionData region = new RegionData();
                        region.setHost(parts[0]);
                        region.setPort(Integer.parseInt(parts[1].trim()));
                        region.setStatus("online");
                        region.setTimestamp(System.currentTimeMillis());
                        log.debug("Parsed address format region data: {}", region);
                        return region;
                    } catch (NumberFormatException e) {
                        log.warn("Failed to parse port from: {}", parts[1]);
                    }
                }
            }
            
            // 尝试提取 IP 和端口（处理 "127.0.1.1" 这种特殊格式）
            // 匹配 IPv4 地址:端口格式
            java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
                "(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}|localhost):(\\d+)");
            java.util.regex.Matcher matcher = pattern.matcher(str);
            if (matcher.find()) {
                try {
                    RegionData region = new RegionData();
                    region.setHost(matcher.group(1));
                    region.setPort(Integer.parseInt(matcher.group(2)));
                    region.setStatus("online");
                    region.setTimestamp(System.currentTimeMillis());
                    log.debug("Parsed regex format region data: {}", region);
                    return region;
                } catch (NumberFormatException e) {
                    log.warn("Failed to parse port from regex match");
                }
            }
            
            log.warn("Failed to parse region data: {}", str);
            return null;
        }
        
        @Override
        public String toString() {
            return String.format("RegionData{host='%s', port=%d, status='%s', role='%s', replicationPort=%d, groupId='%s', timestamp=%d}", 
                               host, port, status, role, replicationPort, groupId, timestamp);
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            RegionData that = (RegionData) obj;
            return port == that.port && host.equals(that.host);
        }
        
        @Override
        public int hashCode() {
            return 31 * host.hashCode() + port;
        }
    }
}