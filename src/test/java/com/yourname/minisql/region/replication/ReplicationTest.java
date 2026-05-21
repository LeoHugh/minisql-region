package com.yourname.minisql.region.replication;

import com.yourname.minisql.region.manager.DatabaseManager;
import com.yourname.minisql.region.storage.LSMTreeEngine;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class ReplicationTest {
    private static final org.slf4j.Logger log = 
        org.slf4j.LoggerFactory.getLogger(ReplicationTest.class);
    
    @TempDir
    Path masterDataDir;
    @TempDir
    Path slaveDataDir;
    
    private DatabaseManager master;
    private DatabaseManager slave;
    private ReplicationManager masterReplicationManager;
    private ReplicationManager slaveReplicationManager;
    
    @BeforeEach
    public void setUp() throws Exception {
        System.out.println("\n========== 测试开始 ==========");
        
        // 创建 Master 数据库
        master = new DatabaseManager(masterDataDir.toString());
        System.out.println("✓ Master 数据库已创建，目录: " + masterDataDir);
        
        // 创建 Slave 数据库
        slave = new DatabaseManager(slaveDataDir.toString());
        System.out.println("✓ Slave 数据库已创建，目录: " + slaveDataDir);
        
        // 创建 Master 的 ReplicationManager
        masterReplicationManager = new ReplicationManager(master, "master-1");
        master.setReplicationManager(masterReplicationManager);
        masterReplicationManager.becomeMaster();
        System.out.println("✓ Master ReplicationManager 已启动，复制端口: " + 
                          masterReplicationManager.getReplicationPort());
        
        // 创建 Slave 的 ReplicationManager
        slaveReplicationManager = new ReplicationManager(slave, "slave-1");
        slave.setReplicationManager(slaveReplicationManager);
        
        // 让 Slave 连接到 Master
        String masterAddress = "localhost:" + masterReplicationManager.getReplicationPort();
        slaveReplicationManager.becomeSlave(masterAddress);
        System.out.println("✓ Slave ReplicationManager 已启动，连接到: " + masterAddress);
        
        // 等待连接建立
        Thread.sleep(2000);
        
        System.out.println("========== 设置完成 ==========\n");
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        System.out.println("\n========== 清理 ==========");
        
        if (masterReplicationManager != null) {
            masterReplicationManager.close();
            System.out.println("Master ReplicationManager 已关闭");
        }
        if (slaveReplicationManager != null) {
            slaveReplicationManager.close();
            System.out.println("Slave ReplicationManager 已关闭");
        }
        if (master != null) {
            master.close();
            System.out.println("Master 数据库已关闭");
        }
        if (slave != null) {
            slave.close();
            System.out.println("Slave 数据库已关闭");
        }
        
        System.out.println("========== 清理完成 ==========\n");
    }
    
    @Test
    @DisplayName("测试主从复制基础功能")
    public void testBasicReplication() throws Exception {
        System.out.println("\n>>> 测试主从复制基础功能");
        
        // 步骤1: 在 Master 上创建表
        System.out.println("步骤1: Master 创建表");
        String createResult = master.execute("CREATE TABLE test (id STRING, name STRING)");
        System.out.println("  建表结果: " + createResult);
        assertTrue(createResult.contains("created") || createResult.contains("成功"));
        
        // 等待复制
        Thread.sleep(1000);
        
        // 步骤2: 在 Master 上插入数据
        System.out.println("\n步骤2: Master 插入数据");
        String insertResult = master.execute("INSERT INTO test (id, name) VALUES ('1', 'master-data')");
        System.out.println("  插入结果: " + insertResult);
        assertTrue(insertResult.contains("Inserted row") || insertResult.contains("成功"));
        
        // 等待复制传播
        System.out.println("\n等待复制传播...");
        Thread.sleep(2000);
        
        // 步骤3: 在 Slave 上查询数据
        System.out.println("\n步骤3: Slave 查询数据");
        String selectResult = slave.execute("SELECT * FROM test WHERE id = '1'");
        System.out.println("  查询结果: '" + selectResult + "'");
        
        // 验证
        assertTrue(selectResult.contains("master-data") || selectResult.contains("测试数据"),
                   "Slave 应该包含 Master 插入的数据，实际结果: " + selectResult);
        
        System.out.println("\n✓ 主从复制测试通过！Master 数据已同步到 Slave");
    }
    
    @Test
    @DisplayName("测试多个写入操作复制")
    public void testMultipleWrites() throws Exception {
        System.out.println("\n>>> 测试多个写入操作复制");
        
        // 创建表
        master.execute("CREATE TABLE test_multi (id STRING, name STRING, age INT)");
        Thread.sleep(500);
        
        // 插入多条数据
        for (int i = 1; i <= 5; i++) {
            String sql = String.format("INSERT INTO test_multi (id, name, age) VALUES ('%d', 'user_%d', '%d')", 
                                       i, i, 20 + i);
            System.out.println("Master 插入: " + sql);
            master.execute(sql);
            Thread.sleep(500);
        }
        
        // 等待复制
        Thread.sleep(2000);
        
        // 验证 Slave 有所有数据
        for (int i = 1; i <= 5; i++) {
            String result = slave.execute("SELECT * FROM test_multi WHERE id = '" + i + "'");
            System.out.println("Slave 查询 id=" + i + ": " + result);
            assertTrue(result.contains("user_" + i), "Slave 应该有 user_" + i);
        }
        
        System.out.println("\n✓ 多个写入操作复制测试通过");
    }
    
    @Test
    @DisplayName("测试删除操作复制")
    public void testDeleteReplication() throws Exception {
        System.out.println("\n>>> 测试删除操作复制");
        
        // 创建表并插入数据
        master.execute("CREATE TABLE test_delete (id STRING, name STRING)");
        master.execute("INSERT INTO test_delete (id, name) VALUES ('1', 'to-be-deleted')");
        Thread.sleep(1000);
        
        // 验证 Slave 有数据
        String beforeDelete = slave.execute("SELECT * FROM test_delete WHERE id = '1'");
        System.out.println("删除前 Slave 查询: " + beforeDelete);
        assertTrue(beforeDelete.contains("to-be-deleted"));
        
        // Master 删除
        master.execute("DELETE FROM test_delete WHERE id = '1'");
        System.out.println("Master 已删除数据");
        
        // 等待复制
        Thread.sleep(2000);
        
        // 验证 Slave 数据已删除
        String afterDelete = slave.execute("SELECT * FROM test_delete WHERE id = '1'");
        System.out.println("删除后 Slave 查询: " + afterDelete);
        assertTrue(afterDelete.contains("No row found") || afterDelete.contains("未找到"),
                   "Slave 的数据应该被删除");
        
        System.out.println("\n✓ 删除操作复制测试通过");
    }
    
}