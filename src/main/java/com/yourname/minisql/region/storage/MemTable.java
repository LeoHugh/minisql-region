package com.yourname.minisql.region.storage;

import com.yourname.minisql.region.model.Row;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.*;

public class MemTable {
    private final ConcurrentSkipListMap<byte[], Row> table;
    private final AtomicLong approximateSize;  // 估算内存占用
    private final long maxSize;
    
    public MemTable() {
        this(64 * 1024 * 1024);  // 默认 64MB
    }
    
    public MemTable(long maxSizeBytes) {
        this.table = new ConcurrentSkipListMap<>(ByteArrayComparator.INSTANCE);
        this.approximateSize = new AtomicLong(0);
        this.maxSize = maxSizeBytes;
    }
    
    public void put(byte[] key, Row row) {
        Row old = table.put(key, row);
        long sizeChange = key.length + row.toBytes().length;
        if (old != null) {
            sizeChange -= (key.length + old.toBytes().length);
        }
        approximateSize.addAndGet(sizeChange);
    }
    
    public Row get(byte[] key) {
        return table.get(key);
    }
    
    public void delete(byte[] key) {
        Row removed = table.remove(key);
        if (removed != null) {
            approximateSize.addAndGet(-(key.length + removed.toBytes().length));
        }
    }
    
    public List<Map.Entry<byte[], Row>> scanAll() {
        return new ArrayList<>(table.entrySet());
    }
    
    public boolean needsFlush() {
        return approximateSize.get() >= maxSize;
    }
    
    public int size() {
        return table.size();
    }
    
    public long getApproximateSize() {
        return approximateSize.get();
    }
    
    // 字节数组比较器
    static class ByteArrayComparator implements Comparator<byte[]> {
        public static final ByteArrayComparator INSTANCE = new ByteArrayComparator();
        
        @Override
        public int compare(byte[] o1, byte[] o2) {
            if (o1 == o2) return 0;
            if (o1 == null) return -1;
            if (o2 == null) return 1;
            
            int minLen = Math.min(o1.length, o2.length);
            for (int i = 0; i < minLen; i++) {
                int cmp = (o1[i] & 0xFF) - (o2[i] & 0xFF);
                if (cmp != 0) return cmp;
            }
            return o1.length - o2.length;
        }
    }
}