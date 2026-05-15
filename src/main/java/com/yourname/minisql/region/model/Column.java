package com.yourname.minisql.region.model;

public class Column {
    private String name;
    private DataType type;      // ← 确认有这个声明
    private boolean isPrimaryKey;
    private boolean nullable;    // ← 确认有这个声明
    
    public enum DataType {
        INT, LONG, STRING, BYTES, BOOLEAN
    }
    
    // 构造器1：简化版（供 DatabaseManager 使用）
    public Column(String name, DataType type) {
        this.name = name;
        this.type = type;
        this.isPrimaryKey = false;
        this.nullable = true;
    }
    
    // 构造器2：完整版（所有属性）
    public Column(String name, DataType type, boolean isPrimaryKey, boolean nullable) {
        this.name = name;
        this.type = type;
        this.isPrimaryKey = isPrimaryKey;
        this.nullable = nullable;
    }
    
    // Getters and Setters
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public DataType getType() { return type; }
    public void setType(DataType type) { this.type = type; }
    public boolean isPrimaryKey() { return isPrimaryKey; }
    public void setPrimaryKey(boolean primaryKey) { isPrimaryKey = primaryKey; }
    public boolean isNullable() { return nullable; }
    public void setNullable(boolean nullable) { this.nullable = nullable; }
    
    @Override
    public String toString() {
        // 注意这里用的是 this.type 和 this.nullable
        return String.format("Column{name='%s', type=%s, pk=%s, nullable=%s}", 
                             name, type, isPrimaryKey, nullable);
    }
}