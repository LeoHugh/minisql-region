package com.yourname.minisql.region.ha.masterengine;

import com.yourname.minisql.region.zk.ZkClientManager;
import com.yourname.minisql.region.zk.ZkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 元数据管理器 —— 管理 table → groupId 映射，并通过 Zookeeper 持久化。
 *
 * ZK 路径结构：
 *   /minisql/metadata/tables/<tableName>  -> data = groupId（持久节点）
 *
 * 启动时从 ZK 加载已有映射到内存缓存；
 * assignTableToGroup() 同时写 ZK 和更新缓存；
 * Master 重启后通过 ZK 自动恢复映射，不再丢失。
 */
public class MetadataManager {
    private static final Logger log = LoggerFactory.getLogger(MetadataManager.class);

    /** 内存缓存：table → groupId */
    private final Map<String, String> tableToGroup = new ConcurrentHashMap<>();

    private final ZkClientManager zkClient;

    public MetadataManager() {
        this.zkClient = ZkClientManager.getInstance();
    }

    /**
     * 启动：确保 ZK 路径存在，并加载已有的 table→group 映射
     */
    public void start() throws Exception {
        String tablesPath = ZkConfig.getTablesPath();
        zkClient.ensurePathExists(tablesPath);

        // 加载已有映射
        List<String> tables;
        try {
            tables = zkClient.getChildren(tablesPath);
        } catch (Exception e) {
            log.warn("Failed to list existing tables from ZK: {}", e.getMessage());
            return;
        }

        for (String tableName : tables) {
            String nodePath = tablesPath + "/" + tableName;
            try {
                byte[] data = zkClient.getNodeData(nodePath);
                if (data != null && data.length > 0) {
                    String groupId = new String(data);
                    tableToGroup.put(tableName, groupId);
                    log.info("Loaded table mapping from ZK: '{}' -> group '{}'", tableName, groupId);
                }
            } catch (Exception e) {
                log.warn("Failed to load table mapping for '{}': {}", tableName, e.getMessage());
            }
        }

        log.info("MetadataManager started, loaded {} table mappings from ZK", tableToGroup.size());
    }

    /**
     * 登记 table → group 映射（写 ZK 持久节点 + 更新内存缓存）
     */
    public void assignTableToGroup(String tableName, String groupId) throws Exception {
        String nodePath = ZkConfig.getTablesPath() + "/" + tableName;

        // 写 ZK 持久节点
        try {
            if (zkClient.getClient().checkExists().forPath(nodePath) != null) {
                zkClient.getClient().setData().forPath(nodePath, groupId.getBytes());
            } else {
                zkClient.createPersistentNode(nodePath, groupId.getBytes());
            }
        } catch (Exception e) {
            log.error("Failed to persist table mapping to ZK: '{}' -> '{}'", tableName, groupId, e);
            throw e;
        }

        // 更新内存缓存
        tableToGroup.put(tableName, groupId);
        log.info("Table '{}' assigned to group '{}' (persisted to ZK)", tableName, groupId);
    }

    /**
     * 查询 table 对应的 groupId
     */
    public String getGroupForTable(String tableName) {
        return tableToGroup.get(tableName);
    }

    /**
     * 移除表映射（删除 ZK 节点 + 清理缓存）
     */
    public void removeTableMapping(String tableName) throws Exception {
        String nodePath = ZkConfig.getTablesPath() + "/" + tableName;
        try {
            zkClient.deleteNode(nodePath);
        } catch (Exception e) {
            log.debug("Failed to delete table mapping node: {}", e.getMessage());
        }
        tableToGroup.remove(tableName);
        log.info("Removed table mapping: '{}'", tableName);
    }

    /**
     * 当某个 Group 下线时，清理指向该 Group 的所有表映射
     */
    public void removeGroupFromRouting(String groupId) throws Exception {
        List<String> toRemove = new ArrayList<>();
        for (Map.Entry<String, String> entry : tableToGroup.entrySet()) {
            if (entry.getValue().equals(groupId)) {
                toRemove.add(entry.getKey());
            }
        }
        for (String table : toRemove) {
            removeTableMapping(table);
            log.info("Removed routing: {} -> group '{}'", table, groupId);
        }
    }

    /**
     * 获取表到 Group 的映射快照（只读）
     */
    public Map<String, String> getTableToGroup() {
        return Collections.unmodifiableMap(tableToGroup);
    }

    /**
     * 停止
     */
    public void stop() {
        log.info("MetadataManager stopped ({} table mappings in cache)", tableToGroup.size());
    }
}
