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

    
    public SSTable(int id, String dataDir, List<Map.Entry<byte[], Row>> entries) throws IOException {
        this.id = id;
        this.dataPath = Paths.get(dataDir, String.format("sst_%d.data", id));
        this.indexPath = Paths.get(dataDir, String.format("sst_%d.idx", id));
        this.index = new ConcurrentHashMap<>();
        
        // 写入数据
        writeData(entries);
      
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
                    index.put(new ByteArrayWrapper(key), offset);
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
        String keyStr = new String(key);
        ByteArrayWrapper wrapper = new ByteArrayWrapper(key);
        Long offset = index.get(wrapper);
        
        if (offset == null) {
            log.info("  [SSTable-{}] 内存 Index Map 中没有找到 Key: {} 的偏移量", id, keyStr);
            return null;
        }
        
        log.info("  [SSTable-{}] 内存 Index 命中！Key: {} 的文件偏移量为: {}", id, keyStr, offset);
        
        synchronized (dataFile) {
            dataFile.seek(offset);
            int keyLen = dataFile.readInt();
            byte[] foundKey = new byte[keyLen];
            dataFile.readFully(foundKey);
            
            int rowLen = dataFile.readInt();
            byte[] rowBytes = new byte[rowLen];
            dataFile.readFully(rowBytes);
            
            log.info("  [SSTable-{}] 成功从磁盘读取出 Row 的字节数据，长度: {}", id, rowLen);
            
            Row row = Row.fromBytes(rowBytes);
            log.info("  [SSTable-{}] 反序列化 Row 结果: {}", id, row.getAll());
            return row;
        }
    }
    
    public boolean mayContain(byte[] key) {
        // 简化：总是返回 true，后续可加布隆过滤器
        return true;
    }
    
    public List<Map.Entry<byte[], Row>> scanAll() throws IOException {
        List<Map.Entry<byte[], Row>> result = new ArrayList<>();
        synchronized (dataFile) {
            dataFile.seek(0);
            while (dataFile.getFilePointer() < dataFile.length()) {
                int keyLen = dataFile.readInt();
                byte[] foundKey = new byte[keyLen];
                dataFile.readFully(foundKey);
                
                int rowLen = dataFile.readInt();
                byte[] rowBytes = new byte[rowLen];
                dataFile.readFully(rowBytes);
                
                result.add(new AbstractMap.SimpleEntry<>(foundKey, Row.fromBytes(rowBytes)));
            }
        }
        return result;
    }
    
    public int getId() { return id; }

    
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
    public void destroy() throws IOException {
        close(); 
        Files.deleteIfExists(dataPath);
        Files.deleteIfExists(indexPath);
        log.info("Deleted old SSTable files: {} and {}", dataPath.getFileName(), indexPath.getFileName());
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