package com.yourname.minisql.region.loadbalance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class LoadBalancerTest {

    private LoadBalancer loadBalancer;

    @BeforeEach
    public void setUp() {
        loadBalancer = new LoadBalancer();
    }

    // 辅助方法：快速构建模拟的 RegionGroup 状态
    private RegionGroup createGroup(String groupId, String masterAddr, String[] slaveAddrs) {
        RegionGroup group = new RegionGroup(groupId);
        if (masterAddr != null) {
            group.setMaster(new RegionNode("m-" + groupId, masterAddr));
        }
        if (slaveAddrs != null) {
            for (int i = 0; i < slaveAddrs.length; i++) {
                group.addSlave(new RegionNode("s-" + groupId + "-" + i, slaveAddrs[i]));
            }
        }
        return group;
    }

    @Test
    @DisplayName("Test round robin strategy for group selection")
    public void testRoundRobinStrategy() {
        // 使用 LinkedHashMap 保证迭代顺序的一致性，以确保轮询结果符合预期
        Map<String, RegionGroup> groupMap = new LinkedHashMap<>();
        groupMap.put("group1", createGroup("group1", "localhost:8801", null));
        groupMap.put("group2", createGroup("group2", "localhost:8802", null));

        loadBalancer.setStrategy(LoadBalancer.LoadBalanceStrategy.ROUND_ROBIN);

        // 使用无状态方法
        String groupA = loadBalancer.selectGroup(groupMap, "table1");
        String groupB = loadBalancer.selectGroup(groupMap, "table2");
        String groupC = loadBalancer.selectGroup(groupMap, "table3");

        assertNotNull(groupA);
        assertNotNull(groupB);
        assertNotNull(groupC);

        assertNotEquals(groupA, groupB, "Consecutive calls should pick different groups");
        assertEquals(groupA, groupC, "Third call should wrap around to first group");
    }

    @Test
    @DisplayName("Test hash based strategy for group selection")
    public void testHashStrategy() {
        Map<String, RegionGroup> groupMap = new LinkedHashMap<>();
        groupMap.put("group1", createGroup("group1", "localhost:8801", null));
        groupMap.put("group2", createGroup("group2", "localhost:8802", null));

        loadBalancer.setStrategy(LoadBalancer.LoadBalanceStrategy.HASH);

        String groupForTableA = loadBalancer.selectGroup(groupMap, "tableA");
        String groupForTableA_Again = loadBalancer.selectGroup(groupMap, "tableA");

        assertEquals(groupForTableA, groupForTableA_Again, "Hash strategy should return same group for same table name");
    }

    @Test
    @DisplayName("Test retrieving group info")
    public void testGetGroupInfo() {
        Map<String, RegionGroup> groupMap = new LinkedHashMap<>();
        groupMap.put("group1", createGroup("group1", "localhost:8801", new String[]{"localhost:8802", "localhost:8803"}));

        Map<String, String> tableToGroup = new HashMap<>();
        tableToGroup.put("users", "group1");

        // 使用无状态查询方法
        String info = loadBalancer.getGroupInfoForTable("users", tableToGroup, groupMap);
        assertNotNull(info);
        assertTrue(info.startsWith("group1|"));
        assertTrue(info.contains("master=localhost:8801"));
        assertTrue(info.contains("localhost:8802"));
        assertTrue(info.contains("localhost:8803"));

        String directInfo = loadBalancer.getGroupInfo("group1", groupMap);
        assertEquals(info, directInfo);
    }
    
    @Test
    @DisplayName("Test stats generation")
    public void testGetStats() {
        Map<String, RegionGroup> groupMap = new LinkedHashMap<>();
        groupMap.put("group1", createGroup("group1", "localhost:8801", new String[]{"localhost:8802"}));

        Map<String, String> tableToGroup = new HashMap<>();
        tableToGroup.put("t1", "group1");

        String stats = loadBalancer.getStats(groupMap, tableToGroup);
        assertTrue(stats.contains("Group 'group1'"));
        assertTrue(stats.contains("t1 -> group1"));
        assertTrue(stats.contains("localhost:8801"));
        assertTrue(stats.contains("localhost:8802"));
    }
}