package com.yourname.minisql.region.model;

import java.util.HashMap;
import java.util.Map;

public class Row {
    private Map<String, Object> values;
    private long timestamp;  // MVCC 版本控制
    private boolean deleted = false; // 墓碑标记
    
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
    
    /*public <T> T getAs(String column, Class<T> clazz) {
        return clazz.cast(values.get(column));
    }*/
    
    public Map<String, Object> getAll() {
        return new HashMap<>(values);
    }
    
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    
    public boolean isDeleted() { return deleted; }
    public void setDeleted(boolean deleted) { this.deleted = deleted; }
    
    // 转为字节数组（用于存储到 LSM-Tree）
    public byte[] toBytes() {
        // 简化：用 JSON 序列化，后续可优化为自定义编码
        return com.alibaba.fastjson2.JSON.toJSONBytes(this);
    }
    
    // 从字节数组恢复
    public static Row fromBytes(byte[] bytes) {
        return com.alibaba.fastjson2.JSON.parseObject(bytes, Row.class);
    }

    //供fastjson2使用
    public Map<String, Object> getValues() {
        return values;
    }
    
    public void setValues(Map<String, Object> values) {
        this.values = values;
    }
    
}