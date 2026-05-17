package com.yourname.minisql.region.model;

import java.io.Serializable;

public class Column implements Serializable {
    private static final long serialVersionUID = 1L;

    private String name;
    private DataType type;      // ← 确认有这个声明
    private boolean isPrimaryKey;
    private boolean nullable;    // ← 确认有这个声明
    
    public enum DataType {
        INT, LONG, STRING, BYTES, BOOLEAN,DOUBLE
    }
    
    // constructor with required fields
    public Column(String name, DataType type) {
        this.name = name;
        this.type = type;
        this.isPrimaryKey = false;
        this.nullable = true;
    }
    
    // CONSTRUCTOR WITH ALL FIELDS
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
    
    
}