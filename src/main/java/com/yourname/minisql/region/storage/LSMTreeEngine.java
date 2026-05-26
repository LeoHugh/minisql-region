package com.yourname.minisql.region.storage;

import com.yourname.minisql.region.model.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LSMTreeEngine implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(LSMTreeEngine.class);
    
    private MemTable activeMemTable;
    private WAL wal;
    private final List<SSTable> sstables;  
    private final String dataDir;
    private int nextWalId;
    private int nextSSTId;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    public LSMTreeEngine(String dataDir) throws IOException {
        this.dataDir = dataDir;
        this.sstables = new ArrayList<>();
        this.nextWalId = 0;
        this.nextSSTId = 0;
        
        // 创建数据目录
        Path dir = Paths.get(dataDir);
        if (!Files.exists(dir)) {
            Files.createDirectories(dir);
        }
        
        recover();
    }
    
    public void put(byte[] key, Row row) throws IOException {
        lock.writeLock().lock();
        try {
            // 1. 写 WAL
            wal.logPut(key, row);
            
            // 2. 写 MemTable
            activeMemTable.put(key, row);
            
            // 3. 检查是否需要刷盘
            if (activeMemTable.needsFlush()) {
                flush();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    public Row get(byte[] key) throws IOException {
        String keyStr = new String(key);
        log.info("[Engine Get] 准备查找 Key: {}", keyStr);
        lock.readLock().lock();
        try {
            // 1. 查 MemTable
            Row row = activeMemTable.get(key);
            if (row != null) {
                log.info("[Engine Get] 在 Active MemTable 中找到 Key: {}", keyStr);
                return row.isDeleted() ? null : row;
            }
            
            log.info("[Engine Get] MemTable 未命中，准备遍历 {} 个 SSTable", sstables.size());
            // 2. 查 SSTable（从新到旧）
            for (SSTable sst : sstables) {
                log.info("[Engine Get] 正在查询 SSTable ID: {}", sst.getId());
                if (sst.mayContain(key)) {
                    row = sst.get(key);
                    if (row != null) {
                        log.info("[Engine Get] 在 SSTable ID: {} 中成功找到 Key: {}", sst.getId(), keyStr);
                        return row.isDeleted() ? null : row;
                    }
                    log.info("[Engine Get] SSTable ID: {} 中没有找到 Key: {}", sst.getId(), keyStr);
                }
            }
            log.info("[Engine Get] 所有层级均未找到 Key: {}", keyStr);
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    public List<Map.Entry<byte[], Row>> scanAll() throws IOException {
        lock.readLock().lock();
        try {
            Map<byte[], Row> merged = new TreeMap<>(MemTable.ByteArrayComparator.INSTANCE);
            
            // 1. Scan from oldest SSTable to newest SSTable
            for (int i = sstables.size() - 1; i >= 0; i--) {
                SSTable sst = sstables.get(i);
                List<Map.Entry<byte[], Row>> entries = sst.scanAll();
                for (Map.Entry<byte[], Row> entry : entries) {
                    merged.put(entry.getKey(), entry.getValue());
                }
            }
            
            // 2. Scan from active MemTable
            List<Map.Entry<byte[], Row>> memEntries = activeMemTable.scanAll();
            for (Map.Entry<byte[], Row> entry : memEntries) {
                merged.put(entry.getKey(), entry.getValue());
            }
            
            // 3. Filter out deleted entries
            merged.entrySet().removeIf(entry -> entry.getValue().isDeleted());
            
            return new ArrayList<>(merged.entrySet());
        } finally {
            lock.readLock().unlock();
        }
    }
    
    public void delete(byte[] key) throws IOException {
        lock.writeLock().lock();
        try {
            wal.logDelete(key);
            activeMemTable.delete(key);
            if (activeMemTable.needsFlush()) {
                flush();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    private void flush() throws IOException {
        log.info("Flushing MemTable to SSTable...");
        
        // 1. 创建新的 MemTable 和 WAL
        MemTable oldMemTable = activeMemTable;
        WAL oldWal = wal;
        
        activeMemTable = new MemTable();
        
        
        // 2. 将旧 MemTable 刷成 SSTable
        if (oldMemTable.size() > 0) {
            SSTable sst = new SSTable(nextSSTId++, dataDir, oldMemTable.scanAll());
            sstables.add(0, sst);  // 最新放最前面
        }
        
        // 3. 关闭旧 WAL
        wal = new WAL(dataDir, nextWalId++);
        

        
        // 4. 删除旧 WAL 文件（可选）
        oldWal.destroy();
        
        log.info("Flush complete. Current SSTable count: {}", sstables.size());
        
        // 5. 触发合并（如果 SSTable 太多）
        if (sstables.size() > 5) {
            compact();
        }
    }



    private void compact() throws IOException {
        log.info("Starting compaction...");
        // 简单的合并策略：合并最旧的 N 个 SSTable
        
        lock.writeLock().lock();
        try {
            if (sstables.size() <= 3) return;
            
            // 合并最旧的 3 个
            List<SSTable> toCompact = new ArrayList<>(sstables.subList(sstables.size() - 3, sstables.size()));
            sstables.subList(sstables.size() - 3, sstables.size()).clear();
            
            // 读取所有 entry
            Map<byte[], Row> merged = new TreeMap<>(MemTable.ByteArrayComparator.INSTANCE);
            
            // toCompact 中，索引越小的数据越新，所以从后往前遍历（从旧到新），这样新数据会自动覆盖旧数据
            for (int i = toCompact.size() - 1; i >= 0; i--) {
                SSTable sst = toCompact.get(i);
                List<Map.Entry<byte[], Row>> entries = sst.scanAll();
                for (Map.Entry<byte[], Row> entry : entries) {
                    merged.put(entry.getKey(), entry.getValue());
                }
            }
            
            // 过滤掉带有墓碑标记的 entry (即真正删除)
            merged.entrySet().removeIf(entry -> entry.getValue().isDeleted());
            
            // 创建新的 SSTable
            if (!merged.isEmpty()) {
                List<Map.Entry<byte[], Row>> entries = new ArrayList<>(merged.entrySet());
                SSTable newSst = new SSTable(nextSSTId++, dataDir, entries);
                // 把它放回旧文件的位置，也就是放到列表最后（作为最旧的数据）
                sstables.add(newSst);
            }
            
            // 销毁旧的 SSTable，释放磁盘空间
            for (SSTable sst : toCompact) {
                sst.destroy();
            }
            log.info("Compaction finished.");
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    private void recover() throws IOException {
        // 1. 加载所有 SSTable
        Path dir = Paths.get(dataDir);
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "sst_*.data")) {
            List<Integer> ids = new ArrayList<>();
            for (Path path : stream) {
                String name = path.getFileName().toString();
                int id = Integer.parseInt(name.substring(4, name.lastIndexOf('.')));
                ids.add(id);
            }
            // 按 ID 排序，从大到小（新的在前）
            ids.sort(Collections.reverseOrder());
            for (int id : ids) {
                sstables.add(new SSTable(id, dataDir));
            }
            nextSSTId = ids.isEmpty() ? 0 : ids.get(0) + 1;
        }
        
        // 2. 恢复 WAL 和 MemTable
        activeMemTable = new MemTable();
        
        // 找最新的 WAL
        Path latestWal = null;
        int latestWalId = -1;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "wal_*.log")) {
            for (Path path : stream) {
                String name = path.getFileName().toString();
                int id = Integer.parseInt(name.substring(4, name.lastIndexOf('.')));
                if (id > latestWalId) {
                    latestWalId = id;
                    latestWal = path;
                }
            }
        }
        
        if (latestWal != null) {
            log.info("Recovering from WAL: {}", latestWal);
            WAL recoveryWal = new WAL(dataDir, latestWalId);
            List<WAL.WalEntry> entries = recoveryWal.recover();
            MemTable tempMemTable = new MemTable();
            
            for (WAL.WalEntry entry : entries) {
                if (entry.op == WAL.Operation.PUT) {
                    tempMemTable.put(entry.key, entry.row);
                } else {
                    tempMemTable.delete(entry.key);
                }
                
                if (tempMemTable.needsFlush()) {
                    SSTable sst = new SSTable(nextSSTId++, dataDir, tempMemTable.scanAll());
                    sstables.add(0, sst);
                    tempMemTable = new MemTable(); 
                }
            }
            
            if (tempMemTable.size() > 0) {
                SSTable sst = new SSTable(nextSSTId++, dataDir, tempMemTable.scanAll());
                sstables.add(0, sst);
                log.info("Force flushed recovered MemTable to SSTable.");
            }
            
            // 恢复完成，旧的 WAL 已经完成历史使命，安全销毁！
            recoveryWal.destroy(); 
            nextWalId = latestWalId + 1;
        } else {
            nextWalId = 0;
        }
        
        // 3. 创建全新的、空的运行环境
        activeMemTable = new MemTable();
        wal = new WAL(dataDir, nextWalId++);
        
        log.info("Recovery complete. Active MemTable is clean. SSTable count: {}", sstables.size());
    }
    
    public void printStats() {
        System.out.println("=== LSM Tree Stats ===");
        System.out.println("MemTable entries: " + activeMemTable.size());
        System.out.println("MemTable size: " + activeMemTable.getApproximateSize() / 1024 + " KB");
        System.out.println("SSTable count: " + sstables.size());
        System.out.println("=====================");
    }
    
    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            if (activeMemTable.size() > 0) {
                flush();
            }
            wal.close();
            for (SSTable sst : sstables) {
                sst.close();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}