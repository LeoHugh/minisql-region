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
    private RandomAccessFile raf;  // 用于读取和获取位置
    private long lastPosition = 0;  // 记录最后写入的位置
    
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
        this.raf = new RandomAccessFile(walPath.toFile(), "rw");
        this.lastPosition = raf.length();  // 初始化时记录文件末尾位置
        log.info("WAL initialized at: {}, initial size: {} bytes", walPath, lastPosition);
    }
    
    public synchronized void logPut(byte[] key, Row row) throws IOException {
        writer.writeByte(Operation.PUT.ordinal());
        writer.writeInt(key.length);
        writer.write(key);
        byte[] rowBytes = row.toBytes();
        writer.writeInt(rowBytes.length);
        writer.write(rowBytes);
        writer.flush();
        
        // 更新最后写入位置
        lastPosition = raf.length();
    }
    
    public synchronized void logDelete(byte[] key) throws IOException {
        writer.writeByte(Operation.DELETE.ordinal());
        writer.writeInt(key.length);
        writer.write(key);
        writer.flush();
        
        // 更新最后写入位置
        lastPosition = raf.length();
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
    
    /**
     * 获取从指定位置开始的日志条目
     * @param startPosition 起始位置（字节偏移量）
     * @param maxEntries 最大条目数
     * @return 日志条目列表
     */
    public List<WalEntry> getEntriesFrom(long startPosition, int maxEntries) throws IOException {
        List<WalEntry> entries = new ArrayList<>();
        
        try (RandomAccessFile reader = new RandomAccessFile(walPath.toFile(), "r")) {
            reader.seek(startPosition);
            
            while (entries.size() < maxEntries && reader.getFilePointer() < reader.length()) {
                try {
                    int opOrd = reader.readByte();
                    Operation op = Operation.values()[opOrd];
                    int keyLen = reader.readInt();
                    byte[] key = new byte[keyLen];
                    reader.readFully(key);
                    
                    if (op == Operation.PUT) {
                        int rowLen = reader.readInt();
                        byte[] rowBytes = new byte[rowLen];
                        reader.readFully(rowBytes);
                        Row row = Row.fromBytes(rowBytes);
                        entries.add(new WalEntry(op, key, row));
                    } else {
                        entries.add(new WalEntry(op, key, null));
                    }
                } catch (EOFException e) {
                    break;
                }
            }
        }
        
        log.debug("Retrieved {} entries from position {}", entries.size(), startPosition);
        return entries;
    }
    
    /**
     * 获取从指定位置开始的所有日志条目（用于全量同步）
     */
    public List<WalEntry> getAllEntriesFrom(long startPosition) throws IOException {
        List<WalEntry> entries = new ArrayList<>();
        
        try (RandomAccessFile reader = new RandomAccessFile(walPath.toFile(), "r")) {
            long fileLength = reader.length();
            if (startPosition >= fileLength) {
                return entries;
            }
            
            reader.seek(startPosition);
            
            while (reader.getFilePointer() < fileLength) {
                try {
                    long pos = reader.getFilePointer();
                    int opOrd = reader.readByte();
                    Operation op = Operation.values()[opOrd];
                    int keyLen = reader.readInt();
                    byte[] key = new byte[keyLen];
                    reader.readFully(key);
                    
                    if (op == Operation.PUT) {
                        int rowLen = reader.readInt();
                        byte[] rowBytes = new byte[rowLen];
                        reader.readFully(rowBytes);
                        Row row = Row.fromBytes(rowBytes);
                        entries.add(new WalEntry(op, key, row));
                    } else {
                        entries.add(new WalEntry(op, key, null));
                    }
                } catch (EOFException e) {
                    break;
                }
            }
        }
        
        return entries;
    }
    
    /**
     * 获取当前 WAL 文件大小
     */
    public long getCurrentSize() throws IOException {
        return Files.size(walPath);
    }
    
    /**
     * 获取 WAL 最后写入的位置
     */
    public long getLastPosition() {
        return lastPosition;
    }
    
    /**
     * 刷新缓冲区
     */
    public void sync() throws IOException {
        writer.flush();
        // 可选：强制刷盘
        if (writer instanceof DataOutputStream) {
            // 获取底层的 FileDescriptor 并 sync
            // 注意：这需要访问包装的流，简化处理
        }
    }
    
    /**
     * 关闭 WAL
     */
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
        if (raf != null) {
            raf.close();
        }
    }
    
    /**
     * 删除 WAL 文件（用于清理）
     */
    public void destroy() throws IOException {
        close();
        Files.deleteIfExists(walPath);
        log.info("Deleted WAL file: {}", walPath);
    }
}