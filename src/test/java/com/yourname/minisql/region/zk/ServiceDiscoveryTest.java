package com.yourname.minisql.region.zk;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Master 服务发现功能测试
 */
public class ServiceDiscoveryTest {
    private static final Logger log = LoggerFactory.getLogger(ServiceDiscoveryTest.class);
    
    private ServiceDiscovery serviceDiscovery;
    private ZkClientManager zkClient;
    
    @BeforeEach
    public void setUp() throws Exception {
        zkClient = ZkClientManager.getInstance();
        zkClient.init();
        
        // 清理旧的注册节点
        try {
            zkClient.deleteNode(ZkConfig.ZK_REGIONS_PATH);
        } catch (Exception e) {
            // 忽略
        }
        zkClient.ensurePathExists(ZkConfig.ZK_REGIONS_PATH);
        
        serviceDiscovery = new ServiceDiscovery();
        serviceDiscovery.start();
        
        System.out.println("\n=== Master 服务发现测试开始 ===");
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        if (serviceDiscovery != null) {
            serviceDiscovery.close();
        }
        // 清理测试节点
        try {
            zkClient.deleteNode(ZkConfig.ZK_REGIONS_PATH);
        } catch (Exception e) {
            // 忽略
        }
        System.out.println("=== 测试完成 ===\n");
    }
    
    @Test
    @DisplayName("测试 Master 获取在线 Region 列表")
    public void testGetOnlineRegions() throws Exception {
        System.out.println("\n>>> 测试获取在线 Region 列表");
        
        // 注册 3 个 Region
        for (int i = 1; i <= 3; i++) {
            RegionRegistry registry = new RegionRegistry("localhost", 8880 + i);
            registry.register();
            System.out.println("注册 Region: localhost:" + (8880 + i));
        }
        
        // 等待 ZK 同步
        Thread.sleep(2000);
        
        // 获取在线 Region
        List<ZkConfig.RegionData> regions = serviceDiscovery.getOnlineRegions();
        System.out.println("发现的 Region: " + regions);
        
        assertEquals(3, regions.size());
        assertTrue(regions.stream().anyMatch(r -> r.getPort() == 8881));
        assertTrue(regions.stream().anyMatch(r -> r.getPort() == 8882));
        assertTrue(regions.stream().anyMatch(r -> r.getPort() == 8883));
        
        System.out.println("✓ Master 获取在线 Region 列表成功，共 " + regions.size() + " 个");
    }
    
    @Test
    @DisplayName("测试轮询负载均衡策略")
    public void testRoundRobinLoadBalancing() throws Exception {
        System.out.println("\n>>> 测试轮询负载均衡策略");
        
        // 注册 3 个 Region
        for (int i = 1; i <= 3; i++) {
            RegionRegistry registry = new RegionRegistry("localhost", 8890 + i);
            registry.register();
        }
        
        Thread.sleep(2000);
        
        // 测试轮询
        java.util.Map<String, Integer> distribution = new java.util.HashMap<>();
        for (int i = 0; i < 30; i++) {
            String region = serviceDiscovery.getNextRegionRoundRobin();
            distribution.put(region, distribution.getOrDefault(region, 0) + 1);
            System.out.println("第 " + (i+1) + " 次分配: " + region);
        }
        
        // 验证每个 Region 被分配了约 10 次
        for (int count : distribution.values()) {
            assertEquals(10, count, "轮询应均匀分配");
        }
        
        System.out.println("✓ 轮询负载均衡测试通过: " + distribution);
    }
    
    @Test
    @DisplayName("测试基于表名的哈希策略")
    public void testHashBasedRouting() throws Exception {
        System.out.println("\n>>> 测试基于表名的哈希策略");
        
        // 注册 3 个 Region
        for (int i = 1; i <= 3; i++) {
            RegionRegistry registry = new RegionRegistry("localhost", 8900 + i);
            registry.register();
        }
        
        Thread.sleep(2000);
        
        // 测试不同表名的路由
        java.util.Map<String, String> tableToRegion = new java.util.HashMap<>();
        String[] tables = {"users", "orders", "products", "carts", "payments"};
        
        for (String table : tables) {
            String region = serviceDiscovery.getRegionByTable(table);
            tableToRegion.put(table, region);
            System.out.println("表 '" + table + "' 路由到: " + region);
        }
        
        // 同一张表应该路由到同一个 Region
        for (String table : tables) {
            String region1 = serviceDiscovery.getRegionByTable(table);
            String region2 = serviceDiscovery.getRegionByTable(table);
            assertEquals(region1, region2, "同一张表应路由到同一 Region");
        }
        
        System.out.println("✓ 基于表名的哈希策略测试通过");
    }
    
    @Test
    @DisplayName("测试 Region 上线通知")
    public void testRegionOnlineNotification() throws Exception {
        System.out.println("\n>>> 测试 Region 上线通知");
        
        CountDownLatch latch = new CountDownLatch(1);
        
        serviceDiscovery.addListener(new ServiceDiscovery.RegionChangeListener() {
            @Override
            public void onRegionOnline(ZkConfig.RegionData region) {
                System.out.println("收到上线通知: " + region);
                latch.countDown();
            }
            
            @Override
            public void onRegionOffline(ZkConfig.RegionData region) {
                // 不处理
            }
        });
        
        // 注册新 Region
        RegionRegistry registry = new RegionRegistry("localhost", 9991);
        registry.register();
        
        // 等待通知
        boolean notified = latch.await(5, TimeUnit.SECONDS);
        assertTrue(notified, "应收到 Region 上线通知");
        
        registry.close();
        System.out.println("✓ Region 上线通知测试通过");
    }
    
    @Test
    @DisplayName("测试 Region 下线通知")
    public void testRegionOfflineNotification() throws Exception {
        System.out.println("\n>>> 测试 Region 下线通知");
        
        CountDownLatch latch = new CountDownLatch(1);
        
        serviceDiscovery.addListener(new ServiceDiscovery.RegionChangeListener() {
            @Override
            public void onRegionOnline(ZkConfig.RegionData region) {
                // 不处理
            }
            
            @Override
            public void onRegionOffline(ZkConfig.RegionData region) {
                System.out.println("收到下线通知: " + region);
                latch.countDown();
            }
        });
        
        // 注册 Region
        RegionRegistry registry = new RegionRegistry("localhost", 9992);
        registry.register();
        
        Thread.sleep(1000);
        
        // 注销 Region
        registry.close();
        
        // 等待通知
        boolean notified = latch.await(5, TimeUnit.SECONDS);
        assertTrue(notified, "应收到 Region 下线通知");
        
        System.out.println("✓ Region 下线通知测试通过");
    }
    
    @Test
    @DisplayName("测试无可用 Region 时的处理")
    public void testNoAvailableRegion() throws Exception {
        System.out.println("\n>>> 测试无可用 Region 时的处理");
        
        // 确保没有 Region 注册
        // 等待 ZK 同步
        Thread.sleep(1000);
        
        String region = serviceDiscovery.getNextRegionRoundRobin();
        assertNull(region, "无可用 Region 时应返回 null");
        
        System.out.println("✓ 无可用 Region 时正确处理，返回: " + region);
    }
}