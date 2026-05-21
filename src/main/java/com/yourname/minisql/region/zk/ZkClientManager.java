package com.yourname.minisql.region.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Zookeeper 客户端管理器 - 单例模式
 */
public class ZkClientManager implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(ZkClientManager.class);
    
    private static volatile ZkClientManager instance;
    private CuratorFramework client;
    private boolean initialized = false;
    
    private ZkClientManager() {}
    
    public static ZkClientManager getInstance() {
        if (instance == null) {
            synchronized (ZkClientManager.class) {
                if (instance == null) {
                    instance = new ZkClientManager();
                }
            }
        }
        return instance;
    }
    
    /**
     * 初始化 ZK 客户端
     */
    public synchronized void init() throws Exception {
        if (initialized) {
            return;
        }
        
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(
            ZkConfig.RETRY_INTERVAL_MS,
            ZkConfig.MAX_RETRIES
        );
        
        client = CuratorFrameworkFactory.newClient(
            ZkConfig.ZK_CONNECT_STRING,
            ZkConfig.SESSION_TIMEOUT_MS,
            ZkConfig.CONNECTION_TIMEOUT_MS,
            retryPolicy
        );
        
        client.start();
        
        // 等待连接成功
        client.blockUntilConnected();
        
        // 创建根路径
        ensurePathExists(ZkConfig.ZK_BASE_PATH);
        ensurePathExists(ZkConfig.ZK_REGIONS_PATH);
        ensurePathExists(ZkConfig.ZK_MASTER_PATH);
        
        initialized = true;
        log.info("Zookeeper client initialized, connected to: {}", ZkConfig.ZK_CONNECT_STRING);
    }
    
    /**
     * 确保路径存在
     */
    public void ensurePathExists(String path) throws Exception {
        if (client.checkExists().forPath(path) == null) {
            client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path);
            log.debug("Created ZK path: {}", path);
        }
    }
    
    /**
     * 创建临时节点
     */
    public void createEphemeralNode(String path, byte[] data) throws Exception {
        if (client.checkExists().forPath(path) != null) {
            client.delete().forPath(path);
        }
        client.create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.EPHEMERAL)
            .forPath(path, data);
        log.debug("Created ephemeral node: {}", path);
    }
    
    /**
     * 创建持久节点
     */
    public void createPersistentNode(String path, byte[] data) throws Exception {
        if (client.checkExists().forPath(path) == null) {
            client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path, data);
            log.debug("Created persistent node: {}", path);
        }
    }
    
    /**
     * 获取节点数据
     */
    public byte[] getNodeData(String path) throws Exception {
        if (client.checkExists().forPath(path) == null) {
            return null;
        }
        return client.getData().forPath(path);
    }
    
    /**
     * 获取子节点列表
     */
    public java.util.List<String> getChildren(String path) throws Exception {
        if (client.checkExists().forPath(path) == null) {
            return new java.util.ArrayList<>();
        }
        return client.getChildren().forPath(path);
    }
    
    /**
     * 删除节点
     */
    public void deleteNode(String path) throws Exception {
        if (client.checkExists().forPath(path) != null) {
            client.delete().deletingChildrenIfNeeded().forPath(path);
            log.debug("Deleted node: {}", path);
        }
    }
    
    /**
     * 获取 Curator 客户端（用于高级操作）
     */
    public CuratorFramework getClient() {
        return client;
    }
    
    public boolean isInitialized() {
        return initialized;
    }
    
    @Override
    public synchronized void close() throws IOException {
        if (client != null) {
            client.close();
            initialized = false;
            client = null;
            instance = null;
            log.info("Zookeeper client closed");
        }
    }
}