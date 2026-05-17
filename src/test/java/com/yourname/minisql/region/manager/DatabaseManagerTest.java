package com.yourname.minisql.region.manager;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class DatabaseManagerTest {

    private DatabaseManager db;

    // 💡 黑科技：JUnit 会自动创建一个临时目录，并注入到这个变量里，测试结束自动删除！
    @TempDir
    Path tempDir; 

    // @BeforeEach 表示在执行每一个 @Test 之前，都会运行这段代码
    @BeforeEach
    public void setUp() throws Exception {
        // 在干净的临时目录中启动全新的数据库
        db = new DatabaseManager(tempDir.toString());
        
        // 预先准备好表结构，省得每个测试里都要写一遍
        String createRes = db.execute("CREATE TABLE students (id INT, name VARCHAR, age INT)");
        assertTrue(createRes.contains("created successfully"), "建表应该成功");
    }

    // @AfterEach 表示在执行每一个 @Test 之后，都会运行这段代码
    @AfterEach
    public void tearDown() throws Exception {
        if (db != null) {
            db.close(); // 安全关闭资源
        }
    }

    @Test
    @DisplayName("测试正常的数据插入与精准查询")
    public void testInsertAndSelect() {
        // 1. 插入数据
        String insertRes = db.execute("INSERT INTO students (id, name, age) VALUES ('1001', 'Alice', '20')");
        assertTrue(insertRes.contains("Inserted row with key: 1001"), "插入应该返回成功信息");

        // 2. 查询验证
        String selectRes = db.execute("SELECT * FROM students WHERE id = '1001'");
        assertNotNull(selectRes);
        assertTrue(selectRes.contains("Alice"), "查询结果必须包含 Alice");
        assertTrue(selectRes.contains("20"), "查询结果必须包含年龄 20");
    }

    @Test
    @DisplayName("测试防呆机制：插入时不提供主键必须报错")
    public void testInsertWithoutPrimaryKey() {
        String errorRes = db.execute("INSERT INTO students (name, age) VALUES ('Bob', '22')");
        assertTrue(errorRes.contains("Primary key is required"), "应该被防呆机制拦截并报错");
    }

    @Test
    @DisplayName("测试数据更新功能")
    public void testUpdate() {
        // 先插入
        db.execute("INSERT INTO students (id, name, age) VALUES ('1002', 'Charlie', '25')");
        
        // 执行更新
        String updateRes = db.execute("UPDATE students SET age = '26' WHERE id = '1002'");
        assertTrue(updateRes.contains("Updated row"), "更新操作应该成功");

        // 验证更新结果
        String selectRes = db.execute("SELECT * FROM students WHERE id = '1002'");
        assertTrue(selectRes.contains("26"), "年龄应该被更新为 26");
        assertFalse(selectRes.contains("25"), "旧年龄 25 不该存在了");
    }

    @Test
    @DisplayName("测试墓碑删除机制")
    public void testDelete() {
        // 先插入
        db.execute("INSERT INTO students (id, name, age) VALUES ('1003', 'David', '30')");
        
        // 确认能查到
        assertTrue(db.execute("SELECT * FROM students WHERE id = '1003'").contains("David"));

        // 执行删除
        String deleteRes = db.execute("DELETE FROM students WHERE id = '1003'");
        assertTrue(deleteRes.contains("Deleted row"), "删除操作应该成功");

        // 再次查询验证，应该触发 Tombstone 拦截
        String selectRes = db.execute("SELECT * FROM students WHERE id = '1003'");
        assertTrue(selectRes.contains("No row found"), "被删除的数据不该被查出来");
    }



    @Test
    @DisplayName("测试大批量写入触发多次 Flush 生成多个 SSTable")
    public void testMultipleSSTables() {
        System.out.println("--- 开始执行大批量写入测试 ---");
        
        // 1. 插入 3000 条数据
        // 每条数据加上元数据大概几十上百字节，3000 条足以撑爆 64KB 的 MemTable 多次
        int totalRecords = 3000;
        for (int i = 1; i <= totalRecords; i++) {
            // 构造 SQL：比如 INSERT INTO students (id, name, age) VALUES ('1', 'User_1', '20')
            String sql = String.format("INSERT INTO students (id, name, age) VALUES ('%d', 'User_%d', '%d')", 
                                        i, i, 20 + (i % 10));
            String result = db.execute(sql);
            assertTrue(result.contains("Inserted row"), "第 " + i + " 条数据插入失败");
        }
        // 打印此时的引擎状态，你应该能在这里看到 SSTable count > 1
        System.out.println("插入 3000 条数据后的引擎状态：");
        db.printStats();

        // 2. 验证数据读取（跨越不同的 SSTable 和 MemTable）
        
        // 查一条最老的数据（大概率已经落入最底层的 SSTable）
        String oldestRes = db.execute("SELECT * FROM students WHERE id = '1'");
        assertTrue(oldestRes.contains("User_1"), "无法从旧 SSTable 中读出最早的数据");

        // 查一条中间的数据（大概率在中间层的 SSTable）
        String middleRes = db.execute("SELECT * FROM students WHERE id = '1500'");
        assertTrue(middleRes.contains("User_1500"), "无法读出中间的数据");

        // 查一条最新的数据（大概率还在当前的 activeMemTable 里）
        String newestRes = db.execute("SELECT * FROM students WHERE id = '3000'");
        assertTrue(newestRes.contains("User_3000"), "无法从 MemTable 中读出最新数据");

        // 3. 测试跨 SSTable 的修改和删除
        
        // 修改一条旧数据（这会在当前的 MemTable 写入一个新版本，掩盖底层的旧版本）
        db.execute("UPDATE students SET age = '99' WHERE id = '10'");
        String updatedRes = db.execute("SELECT * FROM students WHERE id = '10'");
        assertTrue(updatedRes.contains("99"), "跨 SSTable 的更新机制失效");

        // 删除一条旧数据（写入墓碑）
        db.execute("DELETE FROM students WHERE id = '20'");
        String deletedRes = db.execute("SELECT * FROM students WHERE id = '20'");
        assertTrue(deletedRes.contains("No row found"), "跨 SSTable 的删除机制失效");
        
        System.out.println("--- 多 SSTable 读写测试通过 ---");
    }

    
}


