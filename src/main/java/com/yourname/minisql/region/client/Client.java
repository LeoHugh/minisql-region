package com.yourname.minisql.region.client;

import com.yourname.minisql.region.network.NetworkConst;
import com.yourname.minisql.region.network.protocol.Message;

import java.io.*;
import java.net.Socket;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicLong;

public class Client {
    private final String masterHost;
    private final int masterPort;
    private final AtomicLong requestIdGen = new AtomicLong(1);
    
    public Client(String masterHost, int masterPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }
    
    // 向 Master 发送请求，获取 Region 地址
    private String sendToMaster(int requestType, String tableName) throws IOException {
        String body = requestType + " " + tableName;
        Message request = Message.createRequest(
            requestIdGen.getAndIncrement(),
            NetworkConst.MessageType.REQUEST,
            body.getBytes()
        );
        
        try (Socket socket = new Socket(masterHost, masterPort);
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
    
    // 向 Region 发送 SQL 请求
    private String sendToRegion(String regionAddr, String sql) throws IOException {
        String[] parts = regionAddr.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        
        Message request = Message.createRequest(
            requestIdGen.getAndIncrement(),
            NetworkConst.MessageType.REQUEST,
            sql.getBytes()
        );
        
        try (Socket socket = new Socket(host, port);
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
    
    public void execute(String sql) {
        try {
            // 简单解析表名（实际需要完善）
            String tableName = extractTableName(sql);
            if (tableName == null) {
                System.out.println("Error: Cannot extract table name");
                return;
            }
            
            // 判断是建表还是普通 SQL
            boolean isCreateTable = sql.trim().toUpperCase().startsWith("CREATE");
            
            // 第一步：向 Master 获取 Region 地址
            int requestType = isCreateTable ? 
                NetworkConst.RequestType.CREATE_TABLE : 
                NetworkConst.RequestType.GET_REGION;
            
            String regionAddr = sendToMaster(requestType, tableName);
            System.out.println("Got region address: " + regionAddr);
            
            // 第二步：向 Region 执行 SQL
            String result = sendToRegion(regionAddr, sql);
            System.out.println("Result: " + result);
            
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private String extractTableName(String sql) {
        String upper = sql.trim().toUpperCase();
        if (upper.startsWith("CREATE TABLE")) {
            // CREATE TABLE users -> users
            String[] parts = sql.trim().split("\\s+");
            return parts[2];
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
                return parts[0];
            }
        } else if (upper.startsWith("DELETE FROM")) {
            String[] parts = sql.trim().split("\\s+");
            return parts[2];
        }
        return null;
    }
    
    public static void main(String[] args) {
        Client client = new Client("localhost", NetworkConst.MASTER_PORT);
        Scanner scanner = new Scanner(System.in);
        
        System.out.println("=== MiniSQL Client ===");
        System.out.println("Connected to master at localhost:" + NetworkConst.MASTER_PORT);
        System.out.println("Enter SQL commands (type 'exit' to quit):");
        
        while (true) {
            System.out.print("sql> ");
            String sql = scanner.nextLine().trim();
            if (sql.equalsIgnoreCase("exit")) {
                break;
            }
            if (sql.isEmpty()) {
                continue;
            }
            client.execute(sql);
        }
        System.out.println("Goodbye!");
    }
}