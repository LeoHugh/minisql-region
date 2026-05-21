package com.yourname.minisql.region.replication;

/**
 * 主从复制协议定义
 */
public class ReplicationProtocol {
    
    // 复制命令类型
    public static class Command {
        public static final byte DATA_SYNC = 1;      // 数据同步
        public static final byte HEARTBEAT = 2;      // 心跳
        public static final byte GET_STATUS = 3;     // 获取状态
        public static final byte PROMOTE = 4;        // 提升为主
        public static final byte DEMOTE = 5;         // 降级为从
        public static final byte REPLICATE_SQL = 6;  // SQL 语句复制
    }
    
    // 节点角色
    public static class Role {
        public static final String MASTER = "MASTER";
        public static final String SLAVE = "SLAVE";
        public static final String STANDBY = "STANDBY";
    }
    
    // 同步模式
    public static class SyncMode {
        public static final String ASYNC = "ASYNC";   // 异步
        public static final String SEMI_SYNC = "SEMI_SYNC"; // 半同步
        public static final String SYNC = "SYNC";     // 全同步
    }
}