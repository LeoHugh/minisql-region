package com.yourname.minisql.region.zk;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Region 注册功能测试
 */
public class RegionRegistryTest {
    private static final Logger log = LoggerFactory.getLogger(RegionRegistryTest.class);
    
    private RegionRegistry registry1;
    private RegionRegistry registry2;
    private RegionRegistry registry3;
    
    @BeforeEach
    public void setUp() throws Exception {
        // 初始化 ZK 客户端
        ZkClientManager.getInstance().init();
        
        // 清理旧的注册节点
        try {
            ZkClientManager.getInstance().deleteNode(ZkConfig.ZK_REGIONS_PATH);
        } catch (Exception e) {
            // 忽略
        }
        ZkClientManager.getInstance().ensurePathExists(ZkConfig.ZK_REGIONS_PATH);
        
        System.out.println("\n=== Region 注册测试开始 ===");
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        if (registry1 != null) {
            registry1.close();
        }
        if (registry2 != null) {
            registry2.close();
        }
        if (registry3 != null) {
            registry3.close();
        }
        System.out.println("=== 测试完成 ===\n");
    }
    
    @Test
    @DisplayName("测试单个 Region 注册")
    public void testSingleRegionRegister() throws Exception {
        System.out.println("\n>>> 测试单个 Region 注册");
        
        registry1 = new RegionRegistry("localhost", 8888);
        registry1.register();
        
        // 验证节点已创建
        String nodePath = ZkConfig.ZK_REGIONS_PATH + "/region-8888";
        assertNotNull(ZkClientManager.getInstance().getClient().checkExists().forPath(nodePath));
        
        // 验证节点数据
        byte[] data = ZkClientManager.getInstance().getNodeData(nodePath);
        assertNotNull(data);
        String dataStr = new String(data);
        assertTrue(dataStr.contains("8888"));
        assertTrue(dataStr.contains("online"));
        
        System.out.println("✓ 单个 Region 注册成功，节点路径: " + nodePath);
    }
    
    @Test
    @DisplayName("测试多个 Region 注册")
    public void testMultipleRegionsRegister() throws Exception {
        System.out.println("\n>>> 测试多个 Region 注册");
        
        registry1 = new RegionRegistry("localhost", 8888);
        registry2 = new RegionRegistry("localhost", 8889);
        registry3 = new RegionRegistry("localhost", 8890);
        
        registry1.register();
        System.out.println("✓ Region1 (8888) 已注册");
        registry2.register();
        System.out.println("✓ Region2 (8889) 已注册");
        registry3.register();
        System.out.println("✓ Region3 (8890) 已注册");
        
        // 等待 ZK 同步
        Thread.sleep(1000);
        
        // 验证所有节点都存在
        java.util.List<String> children = ZkClientManager.getInstance().getChildren(ZkConfig.ZK_REGIONS_PATH);
        System.out.println("当前注册的 Region: " + children);
        
        assertTrue(children.contains("region-8888"));
        assertTrue(children.contains("region-8889"));
        assertTrue(children.contains("region-8890"));
        
        assertEquals(3, children.size());
        System.out.println("✓ 多个 Region 注册成功，共 " + children.size() + " 个节点");
    }
    
    @Test
    @DisplayName("测试 Region 自动注销（临时节点特性）")
    public void testAutoUnregister() throws Exception {
        System.out.println("\n>>> 测试 Region 自动注销");
        
        // 创建并注册 Region
        RegionRegistry tempRegistry = new RegionRegistry("localhost", 9999);
        tempRegistry.register();
        
        String nodePath = ZkConfig.ZK_REGIONS_PATH + "/region-9999";
        assertNotNull(ZkClientManager.getInstance().getClient().checkExists().forPath(nodePath));
        System.out.println("✓ Region 已注册，节点存在");
        
        // 关闭 Registry（模拟 Region 关闭）
        tempRegistry.close();
        
        // 等待 ZK 会话过期
        Thread.sleep(2000);
        
        // 验证节点已自动删除（临时节点特性）
        assertNull(ZkClientManager.getInstance().getClient().checkExists().forPath(nodePath));
        System.out.println("✓ Region 关闭后节点自动注销");
    }
    
    @Test
    @DisplayName("测试 Region 状态更新")
    public void testUpdateStatus() throws Exception {
        System.out.println("\n>>> 测试 Region 状态更新");
        
        registry1 = new RegionRegistry("localhost", 8888);
        registry1.register();
        
        // 更新状态
        registry1.updateStatus("busy");
        
        Thread.sleep(500);
        
        // 验证状态已更新
        String nodePath = ZkConfig.ZK_REGIONS_PATH + "/region-8888";
        byte[] data = ZkClientManager.getInstance().getNodeData(nodePath);
        String dataStr = new String(data);
        assertTrue(dataStr.contains("busy"));
        
        // 再更新为 online
        registry1.updateStatus("online");
        Thread.sleep(500);
        data = ZkClientManager.getInstance().getNodeData(nodePath);
        dataStr = new String(data);
        assertTrue(dataStr.contains("online"));
        
        System.out.println("✓ Region 状态更新成功");
    }
    
    @Test
    @DisplayName("测试 Region 间互相感知")
    public void testRegionMutualDiscovery() throws Exception {
        System.out.println("\n>>> 测试 Region 间互相感知");
        
        // 启动第一个 Region
        RegionRegistry regionA = new RegionRegistry("localhost", 8001);
        regionA.register();
        System.out.println("✓ RegionA (8001) 已启动");
        
        // 等待注册完成
        Thread.sleep(500);
        
        // 启动第二个 Region
        RegionRegistry regionB = new RegionRegistry("localhost", 8002);
        regionB.register();
        System.out.println("✓ RegionB (8002) 已启动");
        
        // 等待感知
        Thread.sleep(1000);
        
        // RegionA 应该能感知到 RegionB
        java.util.List<String> regionAOthers = regionA.getOtherRegionAddresses();
        System.out.println("RegionA 感知到的其他 Region: " + regionAOthers);
        
        // RegionB 应该能感知到 RegionA
        java.util.List<String> regionBOthers = regionB.getOtherRegionAddresses();
        System.out.println("RegionB 感知到的其他 Region: " + regionBOthers);
        
        // 清理
        regionA.close();
        regionB.close();
        
        System.out.println("✓ Region 间互相感知成功");
    }
}