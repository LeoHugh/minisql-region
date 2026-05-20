package com.yourname.minisql.region.zk;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Zookeeper 客户端基础功能测试
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ZkClientTest {
    
    private ZkClientManager zkClient;
    
    @BeforeEach
    public void setUp() throws Exception {
        zkClient = ZkClientManager.getInstance();
        zkClient.init();
        System.out.println("ZK Client initialized");
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        // 注意：不要关闭 ZK 客户端，因为它是单例的
    }
    
    @Test
    @Order(1)
    @DisplayName("测试 ZK 连接")
    public void testConnection() {
        assertNotNull(zkClient.getClient());
        assertTrue(zkClient.getClient().isStarted());
        System.out.println("✓ ZK 连接成功");
    }
    
    @Test
    @Order(2)
    @DisplayName("测试创建和删除节点")
    public void testCreateAndDeleteNode() throws Exception {
        String testPath = "/minisql/test/test-node";
        String testData = "test-data";
        
        // 创建节点
        zkClient.createEphemeralNode(testPath, testData.getBytes());
        assertNotNull(zkClient.getClient().checkExists().forPath(testPath));
        
        // 读取节点数据
        byte[] data = zkClient.getNodeData(testPath);
        assertEquals(testData, new String(data));
        
        // 删除节点
        zkClient.deleteNode(testPath);
        assertNull(zkClient.getClient().checkExists().forPath(testPath));
        
        System.out.println("✓ 节点创建和删除成功");
    }
    
    @Test
    @Order(3)
    @DisplayName("测试获取子节点列表")
    public void testGetChildren() throws Exception {
        String parentPath = ZkConfig.ZK_REGIONS_PATH;
        zkClient.ensurePathExists(parentPath);
        
        // 创建几个临时节点
        for (int i = 1; i <= 3; i++) {
            String path = parentPath + "/test-region-" + i;
            zkClient.createEphemeralNode(path, ("region" + i).getBytes());
        }
        
        // 获取子节点
        java.util.List<String> children = zkClient.getChildren(parentPath);
        assertTrue(children.size() >= 3);
        assertTrue(children.stream().anyMatch(c -> c.contains("test-region-")));
        
        // 清理
        for (String child : children) {
            if (child.contains("test-region-")) {
                zkClient.deleteNode(parentPath + "/" + child);
            }
        }
        
        System.out.println("✓ 获取子节点列表成功");
    }
}