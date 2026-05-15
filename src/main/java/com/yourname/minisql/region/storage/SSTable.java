package com.yourname.minisql.region.storage;

import com.yourname.minisql.region.model.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SSTable implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(SSTable.class);
    private final int id;
    private final Path dataPath;
    private final Path indexPath;
    private RandomAccessFile dataFile;
    private final Map<ByteArrayWrapper, Long> index;  // key -> offset in data file
    private final long minKey;   // 用于范围查询优化
    private final long maxKey;
    
    public SSTable(int id, String dataDir, List<Map.Entry<byte[], Row>> entries) throws IOException {
        this.id = id;
        this.dataPath = Paths.get(dataDir, String.format("sst_%d.data", id));
        this.indexPath = Paths.get(dataDir, String.format("sst_%d.idx", id));
        this.index = new ConcurrentHashMap<>();
        
        // 写入数据
        writeData(entries);
        
        // 计算 min/max key (这里假设 key 是字符串转 byte[])
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        for (var entry : entries) {
            long keyVal = bytesToLong(entry.getKey());
            min = Math.min(min, keyVal);
            max = Math.max(max, keyVal);
        }
        this.minKey = min;
        this.maxKey = max;
        
        // 打开读取句柄
        this.dataFile = new RandomAccessFile(dataPath.toFile(), "r");
        log.info("Created SSTable {} with {} entries, size: {} bytes", id, entries.size(), Files.size(dataPath));
    }
    
    // 从磁盘加载已存在的 SSTable
    public SSTable(int id, String dataDir) throws IOException {
        this.id = id;
        this.dataPath = Paths.get(dataDir, String.format("sst_%d.data", id));
        this.indexPath = Paths.get(dataDir, String.format("sst_%d.idx", id));
        this.index = new ConcurrentHashMap<>();
        this.dataFile = new RandomAccessFile(dataPath.toFile(), "r");
        
        // 加载索引
        loadIndex();
        
        // 读取 min/max key（可以从索引的第一条和最后一条获取）
        this.minKey = 0;  // 简化，实际需要从索引计算
        this.maxKey = Long.MAX_VALUE;
        
        log.info("Loaded existing SSTable {} from disk", id);
    }
    
    private void writeData(List<Map.Entry<byte[], Row>> entries) throws IOException {
        // 先按 key 排序
        entries.sort(Comparator.comparing(Map.Entry::getKey, MemTable.ByteArrayComparator.INSTANCE));
        
        try (DataOutputStream dataOut = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(dataPath)))) {
            try (DataOutputStream idxOut = new DataOutputStream(
                    new BufferedOutputStream(Files.newOutputStream(indexPath)))) {
                
                for (var entry : entries) {
                    long offset = dataOut.size();  // 当前写入位置
                    byte[] key = entry.getKey();
                    Row row = entry.getValue();
                    
                    // 写入索引
                    idxOut.writeInt(key.length);
                    idxOut.write(key);
                    idxOut.writeLong(offset);
                    
                    // 写入数据
                    dataOut.writeInt(key.length);
                    dataOut.write(key);
                    byte[] rowBytes = row.toBytes();
                    dataOut.writeInt(rowBytes.length);
                    dataOut.write(rowBytes);
                }
            }
        }
    }
    
    private void loadIndex() throws IOException {
        try (DataInputStream idxIn = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(indexPath)))) {
            while (idxIn.available() > 0) {
                int keyLen = idxIn.readInt();
                byte[] key = new byte[keyLen];
                idxIn.readFully(key);
                long offset = idxIn.readLong();
                index.put(new ByteArrayWrapper(key), offset);
            }
        }
        log.info("Loaded {} index entries for SSTable {}", index.size(), id);
    }
    
    public Row get(byte[] key) throws IOException {
        ByteArrayWrapper wrapper = new ByteArrayWrapper(key);
        Long offset = index.get(wrapper);
        if (offset == null) {
            return null;
        }
        
        synchronized (dataFile) {
            dataFile.seek(offset);
            int keyLen = dataFile.readInt();
            byte[] foundKey = new byte[keyLen];
            dataFile.readFully(foundKey);
            
            int rowLen = dataFile.readInt();
            byte[] rowBytes = new byte[rowLen];
            dataFile.readFully(rowBytes);
            
            return Row.fromBytes(rowBytes);
        }
    }
    
    public boolean mayContain(byte[] key) {
        // 简化：总是返回 true，后续可加布隆过滤器
        return true;
    }
    
    public int getId() { return id; }
    public long getMinKey() { return minKey; }
    public long getMaxKey() { return maxKey; }
    
    private long bytesToLong(byte[] bytes) {
        // 简化：假设 key 是数字字符串
        try {
            return Long.parseLong(new String(bytes));
        } catch (NumberFormatException e) {
            return 0;
        }
    }
    
    @Override
    public void close() throws IOException {
        if (dataFile != null) {
            dataFile.close();
        }
    }
    
    // 包装类用于 HashMap 的 byte[] key
    private static class ByteArrayWrapper {
        private final byte[] bytes;
        
        ByteArrayWrapper(byte[] bytes) {
            this.bytes = bytes;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ByteArrayWrapper that = (ByteArrayWrapper) o;
            return Arrays.equals(bytes, that.bytes);
        }
        
        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }
}