package com.yourname.minisql.region.loadbalance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class LoadBalancerTest {

    private LoadBalancer loadBalancer;

    @BeforeEach
    public void setUp() {
        loadBalancer = new LoadBalancer();
    }

    @Test
    @DisplayName("Test node addition and group creation")
    public void testAddRegionToGroup() {
        loadBalancer.addRegionToGroup("group1", "MASTER", "node1", "localhost:8801");
        loadBalancer.addRegionToGroup("group1", "SLAVE", "node2", "localhost:8802");

        Map<String, RegionGroup> groups = loadBalancer.getGroupMap();
        assertEquals(1, groups.size());
        
        RegionGroup group1 = groups.get("group1");
        assertNotNull(group1);
        assertNotNull(group1.getMaster());
        assertEquals("localhost:8801", group1.getMaster().getAddress());
        assertEquals(1, group1.getSlaves().size());
        assertEquals("localhost:8802", group1.getSlaves().get(0).getAddress());
    }

    @Test
    @DisplayName("Test round robin strategy for group selection")
    public void testRoundRobinStrategy() {
        loadBalancer.addRegionToGroup("group1", "MASTER", "node1", "localhost:8801");
        loadBalancer.addRegionToGroup("group2", "MASTER", "node2", "localhost:8802");

        loadBalancer.setStrategy(LoadBalancer.LoadBalanceStrategy.ROUND_ROBIN);

        String groupA = loadBalancer.selectGroup("table1");
        String groupB = loadBalancer.selectGroup("table2");
        String groupC = loadBalancer.selectGroup("table3");

        assertNotNull(groupA);
        assertNotNull(groupB);
        assertNotNull(groupC);
        
        assertNotEquals(groupA, groupB, "Consecutive calls should pick different groups");
        assertEquals(groupA, groupC, "Third call should wrap around to first group");
    }

    @Test
    @DisplayName("Test hash based strategy for group selection")
    public void testHashStrategy() {
        loadBalancer.addRegionToGroup("group1", "MASTER", "node1", "localhost:8801");
        loadBalancer.addRegionToGroup("group2", "MASTER", "node2", "localhost:8802");

        loadBalancer.setStrategy(LoadBalancer.LoadBalanceStrategy.HASH);

        String groupForTableA = loadBalancer.selectGroup("tableA");
        String groupForTableA_Again = loadBalancer.selectGroup("tableA");

        assertEquals(groupForTableA, groupForTableA_Again, "Hash strategy should return same group for same table name");
    }

    @Test
    @DisplayName("Test routing assignment")
    public void testTableToGroupRouting() {
        loadBalancer.addRegionToGroup("group1", "MASTER", "node1", "localhost:8801");
        loadBalancer.assignTableToGroup("users", "group1");

        String masterAddress = loadBalancer.getMasterAddressForTable("users");
        assertEquals("localhost:8801", masterAddress);

        String nextRegion = loadBalancer.getNextRegion("users");
        assertEquals("localhost:8801", nextRegion);
    }

    @Test
    @DisplayName("Test region removal and group cleanup")
    public void testRemoveRegion() {
        loadBalancer.addRegionToGroup("group1", "MASTER", "node1", "localhost:8801");
        loadBalancer.addRegionToGroup("group1", "SLAVE", "node2", "localhost:8802");

        loadBalancer.removeRegion("localhost:8802");
        RegionGroup group1 = loadBalancer.getGroupMap().get("group1");
        assertNotNull(group1);
        assertEquals(0, group1.getSlaves().size());

        loadBalancer.removeRegion("localhost:8801");
        // Group should be removed when empty
        assertNull(loadBalancer.getGroupMap().get("group1"));
    }

    @Test
    @DisplayName("Test getting group info string")
    public void testGetGroupInfoString() {
        loadBalancer.addRegionToGroup("group1", "MASTER", "node1", "localhost:8801");
        loadBalancer.addRegionToGroup("group1", "SLAVE", "node2", "localhost:8802");
        loadBalancer.addRegionToGroup("group1", "SLAVE", "node3", "localhost:8803");
        
        loadBalancer.assignTableToGroup("orders", "group1");
        
        String groupInfo = loadBalancer.getGroupInfoForTable("orders");
        assertNotNull(groupInfo);
        assertTrue(groupInfo.startsWith("group1|"));
        assertTrue(groupInfo.contains("master=localhost:8801"));
        assertTrue(groupInfo.contains("slaves="));
        assertTrue(groupInfo.contains("localhost:8802"));
        assertTrue(groupInfo.contains("localhost:8803"));
    }
}
