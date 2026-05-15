package com.yourname.minisql.region.model;

import java.util.HashMap;
import java.util.Map;

public class Row {
    private Map<String, Object> values;
    private long timestamp;  // MVCC 版本控制
    
    public Row() {
        this.values = new HashMap<>();
        this.timestamp = System.currentTimeMillis();
    }
    
    public void put(String column, Object value) {
        values.put(column, value);
    }
    
    public Object get(String column) {
        return values.get(column);
    }
    
    public <T> T getAs(String column, Class<T> clazz) {
        return clazz.cast(values.get(column));
    }
    
    public Map<String, Object> getAll() {
        return new HashMap<>(values);
    }
    
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    
    // 转为字节数组（用于存储到 LSM-Tree）
    public byte[] toBytes() {
        // 简化：用 JSON 序列化，后续可优化为自定义编码
        return com.alibaba.fastjson2.JSON.toJSONBytes(this);
    }
    
    // 从字节数组恢复
    public static Row fromBytes(byte[] bytes) {
        return com.alibaba.fastjson2.JSON.parseObject(bytes, Row.class);
    }
    
    @Override
    public String toString() {
        return String.format("Row{ts=%d, values=%s}", timestamp, values);
    }
}