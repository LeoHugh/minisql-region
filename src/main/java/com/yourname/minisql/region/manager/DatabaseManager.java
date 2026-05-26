package com.yourname.minisql.region.manager;

import com.yourname.minisql.region.model.*;
import com.yourname.minisql.region.storage.LSMTreeEngine;
import com.yourname.minisql.region.parser.SimpleParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DatabaseManager implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(DatabaseManager.class);
    
    private final LSMTreeEngine storage;
    private final SimpleParser parser;
    private final ConcurrentHashMap<String, Table> tables;
    private final String catalogPath;
    private com.yourname.minisql.region.replication.ReplicationManager replicationManager;

    public DatabaseManager(String dataDir) throws IOException {
        this.storage = new LSMTreeEngine(dataDir);
        this.parser = new SimpleParser();
        this.catalogPath = dataDir + "/catalog.meta";
        this.tables = loadCatalog();
    }
    
    @SuppressWarnings("unchecked")
    private ConcurrentHashMap<String, Table> loadCatalog() {
        File file = new File(catalogPath);
        if (!file.exists()) {
            log.info("Catalog file not found. Creating a clean schema map.");
            return new ConcurrentHashMap<>();
        }
        
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
            ConcurrentHashMap<String, Table> loadedTables = (ConcurrentHashMap<String, Table>) ois.readObject();
            log.info("Successfully loaded {} tables metadata from catalog.", loadedTables.size());
            return loadedTables;
        } catch (Exception e) {
            log.error("Failed to load catalog, starting with empty schema map", e);
            return new ConcurrentHashMap<>();
        }
    }

    private void saveCatalog() {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(catalogPath))) {
            oos.writeObject(tables);
            log.info("Successfully saved catalog metadata with {} tables.", tables.size());
        } catch (IOException e) {
            log.error("Failed to save catalog metadata", e);
        }
    }
    
    public void setReplicationManager(com.yourname.minisql.region.replication.ReplicationManager manager) {
        this.replicationManager = manager;
        log.info("ReplicationManager set to DatabaseManager");
    }

    public LSMTreeEngine getStorage() {
        return this.storage;
    }
    
    public String execute(String sql) {
        try {
            SimpleParser.ParsedSQL parsed = parser.parse(sql);
            log.info("Executing: {} -> {}", sql, parsed);
            
            String result;
            boolean shouldReplicate = false;
            
            switch (parsed.type) {
                case CREATE_TABLE:
                    result = createTable(parsed);
                    // 修复：建表成功时复制（不包含错误信息）
                    shouldReplicate = result.contains("created successfully") || result.contains("created");
                    break;
                    
                case INSERT:
                    result = insert(parsed);
                    shouldReplicate = result.startsWith("Inserted row");
                    break;
                    
                case SELECT:
                    return select(parsed);
                    
                case DELETE:
                    result = delete(parsed);
                    shouldReplicate = result.startsWith("Deleted row");
                    break;
                    
                case UPDATE:
                    result = update(parsed);
                    shouldReplicate = result.startsWith("Updated row");
                    break;
                    
                default:
                    return "Unknown SQL command: " + sql;
            }
            
            // 修复：清晰的复制逻辑
            if (shouldReplicate && replicationManager != null) {
                try {
                    // 注意：检查方法名是否正确
                    replicationManager.replicateSQL(sql);
                    log.debug("SQL replicated successfully: {}", sql);
                } catch (Exception e) {
                    log.error("Failed to replicate SQL: {}", sql, e);
                    // 复制失败不影响主流程，继续返回结果
                }
            } else if (shouldReplicate && replicationManager == null) {
                log.debug("ReplicationManager is null, skipping replication for: {}", sql);
            }
            
            return result;
            
        } catch (Exception e) {
            log.error("Failed to execute SQL: {}", sql, e);
            return "Error: " + e.getMessage();
        }
    }
    
    private String createTable(SimpleParser.ParsedSQL parsed) {
        if (tables.containsKey(parsed.tableName)) {
            return "Error: Table '" + parsed.tableName + "' already exists.";
        }
        
        Table table = new Table(parsed.tableName);
        
        if (parsed.columnDefs == null || parsed.columnDefs.isEmpty()) {
            log.info("No column definitions provided, using default columns");
            Column idColumn = new Column("id", Column.DataType.STRING);
            idColumn.setPrimaryKey(true);
            table.addColumn(idColumn);
            table.addColumn(new Column("name", Column.DataType.STRING));
            table.addColumn(new Column("age", Column.DataType.INT));
        } else {
            for (int i = 0; i < parsed.columnDefs.size(); i++) {
                SimpleParser.ColumnDef def = parsed.columnDefs.get(i);
                Column.DataType type = Column.DataType.STRING;
                if (def.type.toUpperCase().contains("INT")) {
                    type = Column.DataType.INT;
                } else if (def.type.toUpperCase().contains("DOUBLE")) {
                    type = Column.DataType.DOUBLE;
                }
                
                Column column = new Column(def.name, type);
                if (i == 0) {
                    column.setPrimaryKey(true);
                }
                table.addColumn(column);
            }
        }
        
        tables.put(parsed.tableName, table);
        saveCatalog();
        return "Table '" + parsed.tableName + "' created successfully with " + 
               table.getColumns().size() + " columns.";
    }
    
    private String insert(SimpleParser.ParsedSQL parsed) throws IOException {
        Table table = tables.get(parsed.tableName);
        if (table == null) {
            return "Error: Table not found: " + parsed.tableName;
        }
        
        Row row = new Row();
        for (Map.Entry<String, Object> entry : parsed.values.entrySet()) {
            row.put(entry.getKey(), entry.getValue());
        }
        
        String primaryKey = (String) row.get(table.getPrimaryKeyColumn());
        if (primaryKey == null) {
            return "Error: Primary key is required.";
        }
        
        storage.put(primaryKey.getBytes(), row);
        return "Inserted row with key: " + primaryKey;
    }
    
    private String select(SimpleParser.ParsedSQL parsed) throws IOException {
        Table table = tables.get(parsed.tableName);
        if (table == null) {
            return "Error: Table not found: " + parsed.tableName;
        }
        
        if (parsed.primaryKeyValue != null) {
            Row row = storage.get(parsed.primaryKeyValue.getBytes());
            if (row == null) {
                return "No row found with key: " + parsed.primaryKeyValue;
            }
            return formatRow(row);
        } else {
            return "Only point queries are supported in this version";
        }
    }
    
    private String delete(SimpleParser.ParsedSQL parsed) throws IOException {
        if (parsed.primaryKeyValue != null) {
            storage.delete(parsed.primaryKeyValue.getBytes());
            return "Deleted row with key: " + parsed.primaryKeyValue;
        }
        return "Error: Only delete by primary key is supported";
    }
    
    private String update(SimpleParser.ParsedSQL parsed) throws IOException {
        if (parsed.primaryKeyValue == null) {
            return "Error: Only update by primary key is supported";
        }
        
        Row originalRow = storage.get(parsed.primaryKeyValue.getBytes());
        if (originalRow == null) {
            return "Error: No row found with key: " + parsed.primaryKeyValue;
        }
        
        for (Map.Entry<String, Object> entry : parsed.values.entrySet()) {
            originalRow.put(entry.getKey(), entry.getValue());
        }
        
        storage.put(parsed.primaryKeyValue.getBytes(), originalRow);
        return "Updated row with key: " + parsed.primaryKeyValue;
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
        saveCatalog();
        storage.close();
    }
}