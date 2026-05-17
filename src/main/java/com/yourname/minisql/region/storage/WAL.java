package com.yourname.minisql.region.storage;

import com.yourname.minisql.region.model.Row;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WAL implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(WAL.class);
    private final Path walPath;
    private DataOutputStream writer;
    
    public enum Operation {
        PUT, DELETE
    }
    
    public static class WalEntry {
        public Operation op;
        public byte[] key;
        public Row row;  // 对于 delete, row 可为 null
        
        public WalEntry(Operation op, byte[] key, Row row) {
            this.op = op;
            this.key = key;
            this.row = row;
        }
    }
    
    public WAL(String dataDir, int memTableId) throws IOException {
        Path dir = Paths.get(dataDir);
        if (!Files.exists(dir)) {
            Files.createDirectories(dir);
        }
        this.walPath = dir.resolve(String.format("wal_%d.log", memTableId));
        this.writer = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(walPath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)));
        log.info("WAL initialized at: {}", walPath);
    }
    
    public synchronized void logPut(byte[] key, Row row) throws IOException {
        writer.writeByte(Operation.PUT.ordinal());
        writer.writeInt(key.length);
        writer.write(key);
        byte[] rowBytes = row.toBytes();
        writer.writeInt(rowBytes.length);
        writer.write(rowBytes);
        writer.flush();
    }
    
    public synchronized void logDelete(byte[] key) throws IOException {
        writer.writeByte(Operation.DELETE.ordinal());
        writer.writeInt(key.length);
        writer.write(key);
        writer.flush();
    }
    
    public List<WalEntry> recover() throws IOException {
        List<WalEntry> entries = new ArrayList<>();
        if (!Files.exists(walPath)) {
            return entries;
        }
        
        try (DataInputStream dis = new DataInputStream(Files.newInputStream(walPath))) {
            while (dis.available() > 0) {
                int opOrd = dis.readByte();
                Operation op = Operation.values()[opOrd];
                int keyLen = dis.readInt();
                byte[] key = new byte[keyLen];
                dis.readFully(key);
                
                if (op == Operation.PUT) {
                    int rowLen = dis.readInt();
                    byte[] rowBytes = new byte[rowLen];
                    dis.readFully(rowBytes);
                    Row row = Row.fromBytes(rowBytes);
                    entries.add(new WalEntry(op, key, row));
                } else {
                    entries.add(new WalEntry(op, key, null));
                }
            }
        }
        log.info("Recovered {} entries from WAL", entries.size());
        return entries;
    }
    
    public void sync() throws IOException {
        writer.flush();
        // 可选：强制刷盘
        // 在 Linux 上可以调用 writer.getFD().sync()
    }
    
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }

    public void destroy() throws IOException {
        close();
        Files.deleteIfExists(walPath);
        log.info("Deleted old WAL file: {}", walPath);
    }
}