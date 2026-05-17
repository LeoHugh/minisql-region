package com.yourname.minisql.region.model;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.io.Serializable;


public class Table implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private String name;
    private List<Column> columns;
    private String primaryKeyColumn;
    private ConcurrentHashMap<String, Column> columnMap;  // 快速查找

    public Table(String name) {
        this.name = name;
        this.columns = new ArrayList<>();
        this.columnMap = new ConcurrentHashMap<>();
    }

    public Column getColumn(String name) {
        return columnMap.get(name);
    }
    public void addColumn(Column column) {
        columns.add(column);
        columnMap.put(column.getName(), column);
        if (column.isPrimaryKey()) {
            this.primaryKeyColumn = column.getName();
        }
    }
    public List<Column> getColumns() {
        return new ArrayList<>(columns);
    }

    public String getName() { return name; }
    public String getPrimaryKeyColumn() { return primaryKeyColumn; }
    
}