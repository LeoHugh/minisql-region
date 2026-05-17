package com.yourname.minisql.region.parser;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;

import java.util.*;

public class SimpleParser {

    // 稍微扩展一下你的 ParsedSQL，支持列定义（为了修复建表 Bug）
    public static class ParsedSQL {
        public enum Type { CREATE_TABLE, INSERT, SELECT, UPDATE, DELETE, UNKNOWN }
        public Type type;
        public String tableName;
        
        // for CREATE TABLE: 记录列名和数据类型
        public List<ColumnDef> columnDefs; 
        
        // for INSERT/UPDATE
        public Map<String, Object> values;  
        
        // for SELECT
        public List<String> columns;         
        
        // 简化的 WHERE 条件 (如 id = '1')
        public String primaryKeyValue;       
        
        @Override
        public String toString() {
            return "ParsedSQL{type=" + type + ", table='" + tableName + "'}";
        }
    }

    // 内部帮助类：用于传递建表的列信息
    public static class ColumnDef {
        public String name;
        public String type;
        public ColumnDef(String name, String type) { this.name = name; this.type = type; }
    }

    public ParsedSQL parse(String sql) throws Exception {
        ParsedSQL result = new ParsedSQL();
        
        // 核心魔法：这一行代码就把字符串变成了 AST 对象树！
        Statement statement = CCJSqlParserUtil.parse(sql);

        // 使用 instanceof 判断具体的 SQL 类型
        if (statement instanceof CreateTable) {
            parseCreateTable((CreateTable) statement, result);
        } else if (statement instanceof Insert) {
            parseInsert((Insert) statement, result);
        } else if (statement instanceof Select) {
            parseSelect((Select) statement, result);
        } else if (statement instanceof Update) {
            parseUpdate((Update) statement, result);
        } else if (statement instanceof Delete) {
            parseDelete((Delete) statement, result);
        } else {
            result.type = ParsedSQL.Type.UNKNOWN;
        }

        return result;
    }

    private void parseCreateTable(CreateTable createTable, ParsedSQL result) {
        result.type = ParsedSQL.Type.CREATE_TABLE;
        // 直接获取表名，不怕空格，不怕大小写！
        result.tableName = createTable.getTable().getName();
        
        result.columnDefs = new ArrayList<>();
        // 获取括号里的列定义列表
        for (ColumnDefinition colDef : createTable.getColumnDefinitions()) {
            result.columnDefs.add(new ColumnDef(
                    colDef.getColumnName(),
                    colDef.getColDataType().getDataType()
            ));
        }
    }

    private void parseInsert(Insert insert, ParsedSQL result) {
        result.type = ParsedSQL.Type.INSERT;
        result.tableName = insert.getTable().getName();
        
        result.values = new HashMap<>();
        List<Column> columns = insert.getColumns();
        
        // 获取 VALUES 里面的值
        ExpressionList expressions = (ExpressionList) insert.getItemsList();
        List<Expression> values = expressions.getExpressions();
        
        for (int i = 0; i < columns.size(); i++) {
            String colName = columns.get(i).getColumnName();
            // .toString() 会带上引号（比如 'Alice'），你可以选择在这里去掉引号
            String val = values.get(i).toString().replace("'", "");
            result.values.put(colName, val);
        }
    }

    private void parseSelect(Select select, ParsedSQL result) {
        result.type = ParsedSQL.Type.SELECT;
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        
        // 1. 获取表名
        result.tableName = plainSelect.getFromItem().toString();
        
        // 2. 获取 SELECT 后面的字段
        result.columns = new ArrayList<>();
        for (SelectItem item : plainSelect.getSelectItems()) {
            result.columns.add(item.toString());
        }
        
        // 3. 解析 WHERE 条件 (以简单的 id = '1' 为例)
        Expression where = plainSelect.getWhere();
        if (where instanceof EqualsTo) {
            EqualsTo equalsTo = (EqualsTo) where;
            // 左边是字段名，右边是值
            String leftCol = equalsTo.getLeftExpression().toString();
            String rightVal = equalsTo.getRightExpression().toString().replace("'", "");
            
            // 如果你规定目前只支持按 id 查询
            if ("id".equalsIgnoreCase(leftCol)) {
                result.primaryKeyValue = rightVal;
            }
        }
    }

    private void parseUpdate(Update update, ParsedSQL result) {
        result.type = ParsedSQL.Type.UPDATE;
        result.tableName = update.getTable().getName();
        
        result.values = new HashMap<>();
        for (UpdateSet updateSet : update.getUpdateSets()) {
            String colName = updateSet.getColumns().get(0).getColumnName();
            String val = updateSet.getExpressions().get(0).toString().replace("'", "");
            result.values.put(colName, val);
        }
        
        Expression where = update.getWhere();
        if (where instanceof EqualsTo) {
            EqualsTo equalsTo = (EqualsTo) where;
            String leftCol = equalsTo.getLeftExpression().toString();
            String rightVal = equalsTo.getRightExpression().toString().replace("'", "");
            if ("id".equalsIgnoreCase(leftCol)) {
                result.primaryKeyValue = rightVal;
            }
        }
    }

    private void parseDelete(Delete delete, ParsedSQL result) {
        result.type = ParsedSQL.Type.DELETE;
        result.tableName = delete.getTable().getName();
        
        Expression where = delete.getWhere();
        if (where instanceof EqualsTo) {
            EqualsTo equalsTo = (EqualsTo) where;
            String leftCol = equalsTo.getLeftExpression().toString();
            String rightVal = equalsTo.getRightExpression().toString().replace("'", "");
            if ("id".equalsIgnoreCase(leftCol)) {
                result.primaryKeyValue = rightVal;
            }
        }
    }
}