package com.yourname.minisql.region.ha.masterengine;

import com.yourname.minisql.region.ha.masterengine.RegionFailover;
import com.yourname.minisql.region.loadbalance.RegionGroup;
import com.yourname.minisql.region.loadbalance.RegionNode;
import com.yourname.minisql.region.zk.ServiceDiscovery;
import com.yourname.minisql.region.zk.ZkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 集群拓扑管理器 —— 统一管理 Region 上下线、分组拓扑与故障检测。
 *
 * 持有 groupMap（所有 RegionGroup 的全量视图），作为 ServiceDiscovery 的
 * RegionChangeListener 自动同步 ZK 事件；内部创建并管理 RegionFailover
 * 进行主动健康检查。
 *
 * 对外提供只读拓扑查询和可用性/连接计数操作。
 */
public class TopologyManager implements ServiceDiscovery.RegionChangeListener {
    private static final Logger log = LoggerFactory.getLogger(TopologyManager.class);

    /** 核心拓扑数据：groupId → RegionGroup */
    private final Map<String, RegionGroup> groupMap = new ConcurrentHashMap<>();

    private ServiceDiscovery serviceDiscovery;
    private RegionFailover regionFailover;

    public TopologyManager() {}

    // ========== 生命周期 ==========

    /**
     * 启动：初始化 ServiceDiscovery + RegionFailover，加载已有 Region
     */
    public void start() throws Exception {
        // 1. 初始化 ServiceDiscovery
        serviceDiscovery = new ServiceDiscovery();

        // 2. 创建 RegionFailover（主动 TCP 健康检查）
        regionFailover = new RegionFailover(new RegionFailover.FailoverListener() {
            @Override
            public void onRegionFailed(String regionId, String address) {
                log.warn("Region failed: {} ({})", regionId, address);
                markAvailable(address, false);
            }

            @Override
            public void onRegionRecovered(String regionId, String address) {
                log.info("Region recovered: {} ({})", regionId, address);
                markAvailable(address, true);
            }

            @Override
            public void onFailoverCompleted(String fromRegion, String toRegion) {
                log.info("Failover completed: {} -> {}", fromRegion, toRegion);
            }
        });

        // 3. 注册监听器到 ServiceDiscovery
        //    - RegionFailover 先注册，以便接收到已有节点的 onRegionOnline 事件
        serviceDiscovery.addListener(regionFailover);
        //    - TopologyManager 自身作为监听器同步 groupMap
        serviceDiscovery.addListener(this);

        // 4. 启动 ServiceDiscovery（加载已有节点 + 开始监听变化）
        serviceDiscovery.start();

        // 5. 兜底：将已存在的 Region 加载到 groupMap（防止 CuratorCache 事件遗漏）
        for (ZkConfig.RegionData region : serviceDiscovery.getOnlineRegions()) {
            addRegionToGroup(region);
        }

        // 6. 启动健康检查定时任务
        regionFailover.start();

        log.info("TopologyManager started, {} groups loaded", groupMap.size());
    }

    /**
     * 停止
     */
    public void stop() {
        if (regionFailover != null) {
            try {
                regionFailover.stop();
            } catch (Exception e) {
                log.error("Error stopping RegionFailover", e);
            }
            regionFailover = null;
        }

        if (serviceDiscovery != null) {
            try {
                serviceDiscovery.close();
            } catch (Exception e) {
                log.error("Error closing ServiceDiscovery", e);
            }
            serviceDiscovery = null;
        }

        groupMap.clear();
        log.info("TopologyManager stopped");
    }

    // ========== ServiceDiscovery.RegionChangeListener 实现 ==========

    @Override
    public void onRegionOnline(ZkConfig.RegionData region) {
        addRegionToGroup(region);
    }

    @Override
    public void onRegionOffline(ZkConfig.RegionData region) {
        removeRegionFromGroup(region);
    }

    // ========== 内部拓扑操作 ==========

    private void addRegionToGroup(ZkConfig.RegionData region) {
        String address = region.getAddress();
        String regionId = region.getHost() + ":" + region.getPort();
        String groupId = region.getGroupId();
        String role = region.getRole();

        if (groupId == null || groupId.isEmpty()) {
            groupId = "default";
        }
        if (role == null || role.isEmpty()) {
            role = "STANDBY";
        }

        RegionGroup group = groupMap.computeIfAbsent(groupId, RegionGroup::new);
        RegionNode node = new RegionNode(regionId, address);

        if ("MASTER".equalsIgnoreCase(role)) {
            group.setMaster(node);
        } else if ("SLAVE".equalsIgnoreCase(role)) {
            group.addSlave(node);
        } else {
            // STANDBY 等其他角色暂时作为 Slave 处理
            group.addSlave(node);
        }

        log.info("Region online -> group '{}': role={}, addr='{}'", groupId, role, address);
    }

    private void removeRegionFromGroup(ZkConfig.RegionData region) {
        String address = region.getAddress();
        String groupId = region.getGroupId();

        if (groupId != null && !groupId.isEmpty()) {
            RegionGroup group = groupMap.get(groupId);
            if (group != null) {
                group.removeByAddress(address);
                // 如果 Group 已空，移除
                if (group.getMaster() == null && group.getSlaves().isEmpty()) {
                    groupMap.remove(groupId);
                    log.info("Group '{}' removed (empty)", groupId);
                }
            }
            log.info("Region offline <- group '{}': addr='{}'", groupId, address);
        } else {
            // 无 groupId，从所有 Group 中按地址移除
            for (Map.Entry<String, RegionGroup> entry : groupMap.entrySet()) {
                if (entry.getValue().removeByAddress(address)) {
                    RegionGroup g = entry.getValue();
                    if (g.getMaster() == null && g.getSlaves().isEmpty()) {
                        groupMap.remove(entry.getKey());
                        log.info("Group '{}' removed (empty)", entry.getKey());
                    }
                }
            }
            log.info("Region offline (no groupId): addr='{}'", address);
        }
    }

    // ========== 拓扑查询 ==========

    /**
     * 获取所有 Group（只读视图）
     */
    public Map<String, RegionGroup> getGroupMap() {
        return Collections.unmodifiableMap(groupMap);
    }

    /**
     * 获取指定 Group
     */
    public RegionGroup getGroup(String groupId) {
        return groupMap.get(groupId);
    }

    // ========== 可用性管理 ==========

    /**
     * 标记某个 Region 地址的可用状态
     */
    public void markAvailable(String address, boolean available) {
        for (RegionGroup group : groupMap.values()) {
            RegionNode master = group.getMaster();
            if (master != null && master.getAddress().equals(address)) {
                master.setAvailable(available);
                log.info("Region {} in group '{}' is now {}", address, group.getGroupId(),
                        available ? "available" : "unavailable");
                return;
            }
            for (RegionNode slave : group.getSlaves()) {
                if (slave.getAddress().equals(address)) {
                    slave.setAvailable(available);
                    log.info("Region {} in group '{}' is now {}", address, group.getGroupId(),
                            available ? "available" : "unavailable");
                    return;
                }
            }
        }
    }

    // ========== 连接计数 ==========

    public void incrementConnections(String address) {
        findNode(address).ifPresent(RegionNode::incrementConnections);
    }

    public void decrementConnections(String address) {
        findNode(address).ifPresent(RegionNode::decrementConnections);
    }

    private Optional<RegionNode> findNode(String address) {
        for (RegionGroup group : groupMap.values()) {
            RegionNode master = group.getMaster();
            if (master != null && master.getAddress().equals(address)) {
                return Optional.of(master);
            }
            for (RegionNode slave : group.getSlaves()) {
                if (slave.getAddress().equals(address)) {
                    return Optional.of(slave);
                }
            }
        }
        return Optional.empty();
    }
}
