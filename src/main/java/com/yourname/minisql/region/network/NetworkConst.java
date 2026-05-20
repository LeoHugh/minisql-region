package com.yourname.minisql.region.network;

public class NetworkConst {
    public static final int MAGIC = 0x4D53514C;
    public static final int DEFAULT_PORT = 8888;
    public static final int MASTER_PORT = 9999;
    public static final int CLIENT_PORT = 8888;
    
    // 消息类型
    public static class MessageType {
        public static final byte HANDSHAKE = 1;
        public static final byte REQUEST = 2;
        public static final byte RESPONSE = 3;
        public static final byte HEARTBEAT = 4;
    }
    
    // 请求类型
    public static class RequestType {
        public static final byte GET_REGION = 1;
        public static final byte CREATE_TABLE = 2;
        public static final byte SQL_EXECUTE = 10;
    }
    
    // 响应状态
    public static class Status {
        public static final byte SUCCESS = 0;
        public static final byte ERROR = 1;
        public static final byte NOT_FOUND = 2;
    }
}