package com.yourname.minisql.region.parser;

import java.util.*;
import java.util.regex.*;

public class SimpleParser {
    
    // 解析结果封装
    public static class ParsedSQL {
        public enum Type { CREATE_TABLE, INSERT, SELECT, UPDATE, DELETE, UNKNOWN }
        public Type type;
        public String tableName;
        public Map<String, Object> values;  // for INSERT/UPDATE
        public List<String> columns;         // for SELECT
        public Map<String, Object> where;    // 简化的 WHERE 条件
        public String primaryKeyValue;       // 简化的点查: WHERE id = 'xxx'
        
        @Override
        public String toString() {
            return String.format("ParsedSQL{type=%s, table=%s, values=%s, where=%s}", 
                                 type, tableName, values, where);
        }
    }
    
    public ParsedSQL parse(String sql) {
        sql = sql.trim().toUpperCase();
        ParsedSQL result = new ParsedSQL();
        
        if (sql.startsWith("CREATE TABLE")) {
            result.type = ParsedSQL.Type.CREATE_TABLE;
            parseCreateTable(sql, result);
        } else if (sql.startsWith("INSERT")) {
            result.type = ParsedSQL.Type.INSERT;
            parseInsert(sql, result);
        } else if (sql.startsWith("SELECT")) {
            result.type = ParsedSQL.Type.SELECT;
            parseSelect(sql, result);
        } else if (sql.startsWith("UPDATE")) {
            result.type = ParsedSQL.Type.UPDATE;
            parseUpdate(sql, result);
        } else if (sql.startsWith("DELETE")) {
            result.type = ParsedSQL.Type.DELETE;
            parseDelete(sql, result);
        } else {
            result.type = ParsedSQL.Type.UNKNOWN;
        }
        
        return result;
    }
    
    private void parseCreateTable(String sql, ParsedSQL result) {
        // 简化：提取表名
        Pattern pattern = Pattern.compile("CREATE TABLE (\\w+)");
        Matcher matcher = pattern.matcher(sql);
        if (matcher.find()) {
            result.tableName = matcher.group(1);
        }
    }
    
    private void parseInsert(String sql, ParsedSQL result) {
        // INSERT INTO users (id, name) VALUES (1, 'Alice')
        Pattern pattern = Pattern.compile("INSERT INTO (\\w+)\\s*\\(([^)]+)\\)\\s*VALUES\\s*\\(([^)]+)\\)");
        Matcher matcher = pattern.matcher(sql);
        if (matcher.find()) {
            result.tableName = matcher.group(1);
            String[] columns = matcher.group(2).split(",");
            String[] values = matcher.group(3).split(",");
            
            result.values = new HashMap<>();
            for (int i = 0; i < columns.length; i++) {
                String col = columns[i].trim();
                String val = values[i].trim().replace("'", "");
                result.values.put(col, val);
            }
        }
    }
    
    private void parseSelect(String sql, ParsedSQL result) {
        // SELECT * FROM users WHERE id = '1'
        Pattern pattern = Pattern.compile("SELECT (\\S+)\\s+FROM (\\w+)(?:\\s+WHERE\\s+(\\w+)\\s*=\\s*'(\\w+)')?");
        Matcher matcher = pattern.matcher(sql);
        if (matcher.find()) {
            String columnsStr = matcher.group(1);
            result.columns = columnsStr.equals("*") ? null : Arrays.asList(columnsStr.split(","));
            result.tableName = matcher.group(2);
            
            if (matcher.group(3) != null) {
                result.where = new HashMap<>();
                result.where.put(matcher.group(3), matcher.group(4));
                result.primaryKeyValue = matcher.group(4);
            }
        }
    }
    
    private void parseUpdate(String sql, ParsedSQL result) {
        // 简化实现
        result.type = ParsedSQL.Type.UPDATE;
    }
    
    private void parseDelete(String sql, ParsedSQL result) {
        // DELETE FROM users WHERE id = '1'
        Pattern pattern = Pattern.compile("DELETE FROM (\\w+)\\s+WHERE\\s+(\\w+)\\s*=\\s*'(\\w+)'");
        Matcher matcher = pattern.matcher(sql);
        if (matcher.find()) {
            result.tableName = matcher.group(1);
            result.where = new HashMap<>();
            result.where.put(matcher.group(2), matcher.group(3));
            result.primaryKeyValue = matcher.group(3);
        }
    }
}