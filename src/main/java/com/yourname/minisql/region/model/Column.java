package com.yourname.minisql.region.model;

public class Column {
    private String name;
    private DataType dataType;
    private boolean isPrimaryKey;
    private boolean isNullable;
    public Column(String name, DataType dataType, boolean isPrimaryKey, boolean isNullable) {
        this.name = name;
        this.dataType = dataType;
        this.isPrimaryKey = isPrimaryKey;
        this.isNullable = isNullable;
    }
    public enum DataType {
        INT, LONG, STRING, BYTES, BOOLEAN
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
        return String.format("Column{name='%s', type=%s, pk=%s}", name, type, isPrimaryKey);
    }

}