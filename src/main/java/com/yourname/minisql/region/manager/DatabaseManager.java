package com.yourname.minisql.region.manager;

import com.yourname.minisql.region.model.*;
import com.yourname.minisql.region.storage.LSMTreeEngine;
import com.yourname.minisql.region.parser.SimpleParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DatabaseManager implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(DatabaseManager.class);
    
    private final LSMTreeEngine storage;
    private final SimpleParser parser;
    private final ConcurrentHashMap<String, Table> tables;
    
    public DatabaseManager(String dataDir) throws IOException {
        this.storage = new LSMTreeEngine(dataDir);
        this.parser = new SimpleParser();
        this.tables = new ConcurrentHashMap<>();
    }
    
    public String execute(String sql) {
        try {
            SimpleParser.ParsedSQL parsed = parser.parse(sql);
            log.info("Executing: {} -> {}", sql, parsed);
            
            switch (parsed.type) {
                case CREATE_TABLE:
                    return createTable(parsed);
                case INSERT:
                    return insert(parsed);
                case SELECT:
                    return select(parsed);
                case DELETE:
                    return delete(parsed);
                case UPDATE:
                    return update(parsed);
                default:
                    return "Unknown SQL command: " + sql;
            }
        } catch (Exception e) {
            log.error("Failed to execute SQL: {}", sql, e);
            return "Error: " + e.getMessage();
        }
    }
    
    private String createTable(SimpleParser.ParsedSQL parsed) {
        Table table = new Table(parsed.tableName);
        // 简化：预定义一些列
        table.addColumn(new Column("id", Column.DataType.STRING));
        table.addColumn(new Column("name", Column.DataType.STRING));
        table.addColumn(new Column("age", Column.DataType.INT));
        tables.put(parsed.tableName, table);
        return "Table '" + parsed.tableName + "' created";
    }
    
    private String insert(SimpleParser.ParsedSQL parsed) throws IOException {
        Table table = tables.get(parsed.tableName);
        if (table == null) {
            return "Table not found: " + parsed.tableName;
        }
        
        Row row = new Row();
        for (Map.Entry<String, Object> entry : parsed.values.entrySet()) {
            row.put(entry.getKey(), entry.getValue());
        }
        
        // 使用主键作为存储 key
        String primaryKey = (String) row.get(table.getPrimaryKeyColumn());
        if (primaryKey == null) {
            return "Primary key is required";
        }
        
        storage.put(primaryKey.getBytes(), row);
        return "Inserted row with key: " + primaryKey;
    }
    
    private String select(SimpleParser.ParsedSQL parsed) throws IOException {
        Table table = tables.get(parsed.tableName);
        if (table == null) {
            return "Table not found: " + parsed.tableName;
        }
        
        if (parsed.primaryKeyValue != null) {
            // 点查
            Row row = storage.get(parsed.primaryKeyValue.getBytes());
            if (row == null) {
                return "No row found with key: " + parsed.primaryKeyValue;
            }
            return formatRow(row);
        } else {
            // 范围查询等后续实现
            return "Only point queries are supported in this version";
        }
    }
    
    private String delete(SimpleParser.ParsedSQL parsed) throws IOException {
        if (parsed.primaryKeyValue != null) {
            storage.delete(parsed.primaryKeyValue.getBytes());
            return "Deleted row with key: " + parsed.primaryKeyValue;
        }
        return "Only delete by primary key is supported";
    }
    
    private String update(SimpleParser.ParsedSQL parsed) {
        return "UPDATE not implemented yet";
    }
    
    private String formatRow(Row row) {
        StringBuilder sb = new StringBuilder();
        sb.append("Row{");
        for (Map.Entry<String, Object> entry : row.getAll().entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append(", ");
        }
        sb.append("timestamp=").append(row.getTimestamp());
        sb.append("}");
        return sb.toString();
    }
    
    public void printStats() {
        storage.printStats();
    }
    
    @Override
    public void close() throws IOException {
        storage.close();
    }
}