package com.yourname.minisql.region.network.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.yourname.minisql.region.network.NetworkConst.MessageType;

public class Message {
    private static final int MAGIC_NUMBER = 0x4D53514C; // "MSQL"
    
    private int magic;
    private int totalLength;
    private byte type;
    private long requestId;
    private byte status;
    private int bodyLength;
    private byte[] body;
    
    private Message() {}
    
    // 构建请求消息
    public static Message createRequest(long requestId, byte type, byte[] body) {
        Message msg = new Message();
        msg.magic = MAGIC_NUMBER;
        msg.type = type;
        msg.requestId = requestId;
        msg.status = 0;
        msg.body = body == null ? new byte[0] : body;
        msg.bodyLength = msg.body.length;
        msg.totalLength = 4 + 4 + 1 + 8 + 1 + 4 + msg.bodyLength;
        return msg;
    }
    
    // 构建响应消息
    public static Message createResponse(long requestId, byte status, byte[] body) {
        Message msg = new Message();
        msg.magic = MAGIC_NUMBER;
        msg.type = MessageType.RESPONSE;
        msg.requestId = requestId;
        msg.status = status;
        msg.body = body == null ? new byte[0] : body;
        msg.bodyLength = msg.body.length;
        msg.totalLength = 4 + 4 + 1 + 8 + 1 + 4 + msg.bodyLength;
        return msg;
    }
    
    // 编码为字节数组
    public byte[] encode() {
        ByteBuffer buffer = ByteBuffer.allocate(totalLength);
        buffer.putInt(magic);
        buffer.putInt(totalLength);
        buffer.put(type);
        buffer.putLong(requestId);
        buffer.put(status);
        buffer.putInt(bodyLength);
        buffer.put(body);
        return buffer.array();
    }
    
    // 解码字节数组
    public static Message decode(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        Message msg = new Message();
        msg.magic = buffer.getInt();
        msg.totalLength = buffer.getInt();
        msg.type = buffer.get();
        msg.requestId = buffer.getLong();
        msg.status = buffer.get();
        msg.bodyLength = buffer.getInt();
        msg.body = new byte[msg.bodyLength];
        buffer.get(msg.body);
        return msg;
    }
    
    // 辅助方法：获取请求体的字符串内容
    public String getBodyAsString() {
        return new String(body, StandardCharsets.UTF_8);
    }
    
    // Getters
    public byte getType() { return type; }
    public long getRequestId() { return requestId; }
    public byte getStatus() { return status; }
    public byte[] getBody() { return body; }
}