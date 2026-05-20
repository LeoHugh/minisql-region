package com.yourname.minisql.region.network;

import com.yourname.minisql.region.network.protocol.Message;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

public class MessageTest {

    @Test
    @DisplayName("测试请求消息的编码和解码")
    public void testRequestEncodeDecode() {
        // 准备测试数据
        long requestId = 12345L;
        byte type = NetworkConst.MessageType.REQUEST;
        String body = "GET_REGION users";
        byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
        
        // 编码
        Message request = Message.createRequest(requestId, type, bodyBytes);
        byte[] encoded = request.encode();
        
        // 解码
        Message decoded = Message.decode(encoded);
        
        // 验证
        assertEquals(requestId, decoded.getRequestId());
        assertEquals(type, decoded.getType());
        assertEquals(body, decoded.getBodyAsString());
    }
    
    @Test
    @DisplayName("测试响应消息的编码和解码")
    public void testResponseEncodeDecode() {
        long requestId = 67890L;
        byte status = NetworkConst.Status.SUCCESS;
        String body = "localhost:8888";
        
        Message response = Message.createResponse(requestId, status, body.getBytes());
        byte[] encoded = response.encode();
        Message decoded = Message.decode(encoded);
        
        assertEquals(requestId, decoded.getRequestId());
        assertEquals(NetworkConst.MessageType.RESPONSE, decoded.getType());
        assertEquals(status, decoded.getStatus());
        assertEquals(body, decoded.getBodyAsString());
    }
    
    @Test
    @DisplayName("测试空消息体的处理")
    public void testEmptyBody() {
        Message msg = Message.createRequest(1L, NetworkConst.MessageType.HEARTBEAT, null);
        byte[] encoded = msg.encode();
        Message decoded = Message.decode(encoded);
        
        assertEquals(0, decoded.getBody().length);
        assertEquals("", decoded.getBodyAsString());
    }
    
    @Test
    @DisplayName("测试大消息体的处理")
    public void testLargeBody() {
        // 创建一个 10KB 的消息体
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            sb.append('a');
        }
        String largeBody = sb.toString();
        
        Message msg = Message.createRequest(1L, NetworkConst.MessageType.REQUEST, largeBody.getBytes());
        byte[] encoded = msg.encode();
        Message decoded = Message.decode(encoded);
        
        assertEquals(largeBody, decoded.getBodyAsString());
    }
}