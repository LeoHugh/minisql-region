package com.yourname.minisql.region.stress;

import com.yourname.minisql.region.client.Client;
import com.yourname.minisql.region.ha.HAMasterServer;
import com.yourname.minisql.region.manager.DatabaseManager;
import com.yourname.minisql.region.network.RegionServer;
import com.yourname.minisql.region.replication.ReplicationManager;
import com.yourname.minisql.region.zk.RegionRegistry;
import com.yourname.minisql.region.zk.ZkClientManager;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MiniSQL 压力测试
 * 
 * 测试维度：
 *   1. 高并发写入：多线程并发 INSERT
 *   2. 高并发读取：多线程并发 SELECT
 *   3. 混合读写：INSERT / SELECT / UPDATE / DELETE 混合并发
 *   4. 突发流量：短时间内大量并发请求
 *   5. 故障恢复压测：Master 故障切换期间持续发送请求
 * 
 * 每个测试均会输出性能指标：TPS、平均延迟、P50/P95/P99 延迟、错误率
 */
public class StressTest {
    private static final Logger log = LoggerFactory.getLogger(StressTest.class);

    // ======================== 测试参数（可按需调整）========================
    /** 并发写入线程数 */
    private static final int WRITE_CONCURRENCY = 20;
    /** 每个写入线程执行的操作数 */
    private static final int WRITE_OPS_PER_THREAD = 50;

    /** 并发读取线程数 */
    private static final int READ_CONCURRENCY = 30;
    /** 每个读取线程执行的操作数 */
    private static final int READ_OPS_PER_THREAD = 50;

    /** 混合测试线程数 */
    private static final int MIXED_CONCURRENCY = 20;
    /** 混合测试每线程操作数 */
    private static final int MIXED_OPS_PER_THREAD = 40;

    /** 突发流量线程数 */
    private static final int BURST_CONCURRENCY = 50;
    /** 突发流量每线程操作数 */
    private static final int BURST_OPS_PER_THREAD = 20;

    // ======================== 基础设施 ========================
    private HAMasterServer master1;
    private HAMasterServer master2;
    private RegionServer region1;
    private RegionServer region2;
    private RegionRegistry registry1;
    private RegionRegistry registry2;
    private DatabaseManager db1;
    private DatabaseManager db2;
    private ReplicationManager rep1;
    private ReplicationManager rep2;

    // 每个线程用独立 Client，避免并发安全问题
    private final ThreadLocal<Client> threadClient = ThreadLocal.withInitial(
            () -> new Client("localhost", 9999, 2)
    );

    // ======================== 性能统计 ========================
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger errorCount = new AtomicInteger(0);
    private final ConcurrentLinkedQueue<Long> latencies = new ConcurrentLinkedQueue<>();
    private final AtomicLong totalTableCounter = new AtomicLong(0);

    // ======================== setUp / tearDown ========================

    @BeforeEach
    public void setUp() throws Exception {
        System.out.println("\n========== 压力测试 Setup 开始 ==========");

        // 清理旧数据
        cleanupTestData();

        // 初始化 ZK
        ZkClientManager.getInstance().init();
        try {
            ZkClientManager.getInstance().deleteNode("/minisql/master/election");
        } catch (Exception ignored) {}

        // 启动双 Master
        master1 = new HAMasterServer(9999, "master-1");
        master1.start();
        master2 = new HAMasterServer(9998, "master-2");
        master2.start();

        waitForCondition(() -> isPortOpen(9999) || isPortOpen(9998), 5000);

        // 启动 Region1 (MASTER)
        db1 = new DatabaseManager("./stress_test_db_1");
        rep1 = new ReplicationManager(db1, "stress-region-1");
        db1.setReplicationManager(rep1);
        rep1.becomeMaster();
        region1 = new RegionServer(8801, db1, rep1);
        new Thread(() -> { try { region1.start(); } catch (Exception e) { log.error("Region1 启动失败", e); } }).start();
        registry1 = new RegionRegistry("localhost", 8801, "stress-group");
        registry1.register();
        registry1.updateReplicationInfo("MASTER", rep1.getReplicationPort());

        // 启动 Region2 (SLAVE)
        db2 = new DatabaseManager("./stress_test_db_2");
        rep2 = new ReplicationManager(db2, "stress-region-2");
        db2.setReplicationManager(rep2);
        rep2.becomeSlave("localhost:" + rep1.getReplicationPort());
        region2 = new RegionServer(8802, db2, rep2);
        new Thread(() -> { try { region2.start(); } catch (Exception e) { log.error("Region2 启动失败", e); } }).start();
        registry2 = new RegionRegistry("localhost", 8802, "stress-group");
        registry2.register();
        registry2.updateReplicationInfo("SLAVE", rep2.getReplicationPort());

        // 等待所有 Region 可用
        waitForCondition(() -> isPortOpen(8801) && isPortOpen(8802), 5000);
        Thread.sleep(1500); // 等 LoadBalancer 完成刷新

        System.out.println("========== 压力测试 Setup 完成 ==========\n");
    }

    @AfterEach
    public void tearDown() throws Exception {
        System.out.println("\n========== 压力测试 Teardown 开始 ==========");

        // 按顺序关闭：Registry → Region → Master → DB
        safeClose("Registry1", () -> registry1.close());
        safeClose("Registry2", () -> registry2.close());
        safeClose("Region1", () -> region1.stop());
        safeClose("Region2", () -> region2.stop());
        safeClose("Master1", () -> master1.stop());
        safeClose("Master2", () -> master2.stop());
        safeClose("DB1", () -> db1.close());
        safeClose("DB2", () -> db2.close());

        // 重置统计
        successCount.set(0);
        errorCount.set(0);
        latencies.clear();

        System.out.println("========== 压力测试 Teardown 完成 ==========\n");
    }

    // ======================== 测试用例 ========================

    @Test
    @DisplayName("压力测试1: 高并发写入")
    public void testHighConcurrencyWrite() throws Exception {
        System.out.println("\n╔══════════════════════════════════════════╗");
        System.out.println("║    压力测试1: 高并发写入                   ║");
        System.out.println("╚══════════════════════════════════════════╝");

        String tableName = "stress_write_" + totalTableCounter.incrementAndGet();

        // 建表
        Client setupClient = new Client("localhost", 9999, 3);
        String createResult = setupClient.execute("CREATE TABLE " + tableName + " (id STRING, name STRING, value STRING)");
        System.out.println("建表结果: " + createResult);
        assertTrue(createResult.contains("created"), "建表失败: " + createResult);
        Thread.sleep(500);

        // 并发写入
        int totalOps = WRITE_CONCURRENCY * WRITE_OPS_PER_THREAD;
        System.out.println("开始并发写入: " + WRITE_CONCURRENCY + " 线程 × " + WRITE_OPS_PER_THREAD + " 操作 = " + totalOps + " 次 INSERT");

        long startTime = System.currentTimeMillis();

        ExecutorService executor = Executors.newFixedThreadPool(WRITE_CONCURRENCY);
        CountDownLatch latch = new CountDownLatch(WRITE_CONCURRENCY);

        for (int t = 0; t < WRITE_CONCURRENCY; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    Client client = new Client("localhost", 9999, 2);
                    client.setRetryIntervalMs(200);
                    for (int i = 0; i < WRITE_OPS_PER_THREAD; i++) {
                        String key = "t" + threadId + "_" + i;
                        String sql = "INSERT INTO " + tableName + " (id, name, value) VALUES ('" 
                                + key + "', 'user_" + key + "', 'val_" + i + "')";
                        executeSqlWithMetrics(client, sql);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(120, TimeUnit.SECONDS);
        executor.shutdown();
        long duration = System.currentTimeMillis() - startTime;

        // 输出报告
        printReport("高并发写入", totalOps, duration);

        // 断言
        double errorRate = (double) errorCount.get() / totalOps;
        assertTrue(errorRate < 0.10, "错误率过高: " + String.format("%.2f%%", errorRate * 100));
    }

    @Test
    @DisplayName("压力测试2: 高并发读取")
    public void testHighConcurrencyRead() throws Exception {
        System.out.println("\n╔══════════════════════════════════════════╗");
        System.out.println("║    压力测试2: 高并发读取                   ║");
        System.out.println("╚══════════════════════════════════════════╝");

        String tableName = "stress_read_" + totalTableCounter.incrementAndGet();

        // 建表 + 预先插入数据
        Client setupClient = new Client("localhost", 9999, 3);
        setupClient.execute("CREATE TABLE " + tableName + " (id STRING, name STRING)");
        Thread.sleep(500);

        int seedDataCount = 100;
        System.out.println("预插入 " + seedDataCount + " 条种子数据...");
        for (int i = 0; i < seedDataCount; i++) {
            setupClient.execute("INSERT INTO " + tableName + " (id, name) VALUES ('" + i + "', 'seed_" + i + "')");
        }
        Thread.sleep(500);
        System.out.println("种子数据插入完成");

        // 并发读取
        int totalOps = READ_CONCURRENCY * READ_OPS_PER_THREAD;
        System.out.println("开始并发读取: " + READ_CONCURRENCY + " 线程 × " + READ_OPS_PER_THREAD + " 操作 = " + totalOps + " 次 SELECT");

        long startTime = System.currentTimeMillis();
        ExecutorService executor = Executors.newFixedThreadPool(READ_CONCURRENCY);
        CountDownLatch latch = new CountDownLatch(READ_CONCURRENCY);
        Random random = new Random();

        for (int t = 0; t < READ_CONCURRENCY; t++) {
            executor.submit(() -> {
                try {
                    Client client = new Client("localhost", 9999, 2);
                    client.setRetryIntervalMs(200);
                    for (int i = 0; i < READ_OPS_PER_THREAD; i++) {
                        int key = random.nextInt(seedDataCount);
                        String sql = "SELECT * FROM " + tableName + " WHERE id = '" + key + "'";
                        executeSqlWithMetrics(client, sql);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(120, TimeUnit.SECONDS);
        executor.shutdown();
        long duration = System.currentTimeMillis() - startTime;

        printReport("高并发读取", totalOps, duration);

        double errorRate = (double) errorCount.get() / totalOps;
        assertTrue(errorRate < 0.10, "错误率过高: " + String.format("%.2f%%", errorRate * 100));
    }

    @Test
    @DisplayName("压力测试3: 混合读写（CRUD）")
    public void testMixedCRUD() throws Exception {
        System.out.println("\n╔══════════════════════════════════════════╗");
        System.out.println("║    压力测试3: 混合读写 (CRUD)              ║");
        System.out.println("╚══════════════════════════════════════════╝");

        String tableName = "stress_mixed_" + totalTableCounter.incrementAndGet();

        Client setupClient = new Client("localhost", 9999, 3);
        setupClient.execute("CREATE TABLE " + tableName + " (id STRING, name STRING, age STRING)");
        Thread.sleep(500);

        // 预插入部分数据
        for (int i = 0; i < 50; i++) {
            setupClient.execute("INSERT INTO " + tableName + " (id, name, age) VALUES ('" + i + "', 'init_" + i + "', '" + (20 + i) + "')");
        }
        Thread.sleep(500);

        int totalOps = MIXED_CONCURRENCY * MIXED_OPS_PER_THREAD;
        System.out.println("开始混合 CRUD: " + MIXED_CONCURRENCY + " 线程 × " + MIXED_OPS_PER_THREAD + " 操作 = " + totalOps + " 次");
        System.out.println("操作比例: INSERT 30% | SELECT 40% | UPDATE 20% | DELETE 10%");

        AtomicInteger insertCount = new AtomicInteger(0);
        AtomicInteger selectCount = new AtomicInteger(0);
        AtomicInteger updateCount = new AtomicInteger(0);
        AtomicInteger deleteCount = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();
        ExecutorService executor = Executors.newFixedThreadPool(MIXED_CONCURRENCY);
        CountDownLatch latch = new CountDownLatch(MIXED_CONCURRENCY);
        Random random = new Random();

        for (int t = 0; t < MIXED_CONCURRENCY; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    Client client = new Client("localhost", 9999, 2);
                    client.setRetryIntervalMs(200);
                    for (int i = 0; i < MIXED_OPS_PER_THREAD; i++) {
                        int op = random.nextInt(100);
                        String sql;
                        String key = "mix_" + threadId + "_" + i;

                        if (op < 30) {
                            // INSERT 30%
                            sql = "INSERT INTO " + tableName + " (id, name, age) VALUES ('" 
                                    + key + "', 'mixed_" + key + "', '" + random.nextInt(100) + "')";
                            insertCount.incrementAndGet();
                        } else if (op < 70) {
                            // SELECT 40%
                            int readKey = random.nextInt(50);
                            sql = "SELECT * FROM " + tableName + " WHERE id = '" + readKey + "'";
                            selectCount.incrementAndGet();
                        } else if (op < 90) {
                            // UPDATE 20%
                            int updateKey = random.nextInt(50);
                            sql = "UPDATE " + tableName + " SET name = 'updated_" + key + "' WHERE id = '" + updateKey + "'";
                            updateCount.incrementAndGet();
                        } else {
                            // DELETE 10%
                            int deleteKey = random.nextInt(50);
                            sql = "DELETE FROM " + tableName + " WHERE id = '" + deleteKey + "'";
                            deleteCount.incrementAndGet();
                        }

                        executeSqlWithMetrics(client, sql);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(120, TimeUnit.SECONDS);
        executor.shutdown();
        long duration = System.currentTimeMillis() - startTime;

        System.out.println("\n操作分布统计:");
        System.out.println("  INSERT: " + insertCount.get());
        System.out.println("  SELECT: " + selectCount.get());
        System.out.println("  UPDATE: " + updateCount.get());
        System.out.println("  DELETE: " + deleteCount.get());

        printReport("混合读写 (CRUD)", totalOps, duration);

        double errorRate = (double) errorCount.get() / totalOps;
        assertTrue(errorRate < 0.15, "错误率过高: " + String.format("%.2f%%", errorRate * 100));
    }

    @Test
    @DisplayName("压力测试4: 突发流量")
    public void testBurstTraffic() throws Exception {
        System.out.println("\n╔══════════════════════════════════════════╗");
        System.out.println("║    压力测试4: 突发流量                     ║");
        System.out.println("╚══════════════════════════════════════════╝");

        String tableName = "stress_burst_" + totalTableCounter.incrementAndGet();

        Client setupClient = new Client("localhost", 9999, 3);
        setupClient.execute("CREATE TABLE " + tableName + " (id STRING, data STRING)");
        Thread.sleep(500);

        int totalOps = BURST_CONCURRENCY * BURST_OPS_PER_THREAD;
        System.out.println("模拟突发流量: " + BURST_CONCURRENCY + " 线程同时发起，每线程 " + BURST_OPS_PER_THREAD + " 次 = " + totalOps + " 次请求");

        // 使用 CyclicBarrier 确保所有线程同时开始
        CyclicBarrier barrier = new CyclicBarrier(BURST_CONCURRENCY);

        long startTime = System.currentTimeMillis();
        ExecutorService executor = Executors.newFixedThreadPool(BURST_CONCURRENCY);
        CountDownLatch latch = new CountDownLatch(BURST_CONCURRENCY);

        for (int t = 0; t < BURST_CONCURRENCY; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    Client client = new Client("localhost", 9999, 2);
                    client.setRetryIntervalMs(100);
                    // 所有线程在此等待，然后同时冲！
                    barrier.await(10, TimeUnit.SECONDS);
                    
                    for (int i = 0; i < BURST_OPS_PER_THREAD; i++) {
                        String key = "burst_" + threadId + "_" + i;
                        String sql = "INSERT INTO " + tableName + " (id, data) VALUES ('" 
                                + key + "', 'burst_data_" + System.nanoTime() + "')";
                        executeSqlWithMetrics(client, sql);
                    }
                } catch (Exception e) {
                    log.error("突发测试线程异常", e);
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(120, TimeUnit.SECONDS);
        executor.shutdown();
        long duration = System.currentTimeMillis() - startTime;

        printReport("突发流量", totalOps, duration);

        double errorRate = (double) errorCount.get() / totalOps;
        assertTrue(errorRate < 0.20, "突发流量下错误率过高: " + String.format("%.2f%%", errorRate * 100));
    }

    @Test
    @DisplayName("压力测试5: Master 故障切换期间持续请求")
    public void testFailoverUnderLoad() throws Exception {
        System.out.println("\n╔══════════════════════════════════════════╗");
        System.out.println("║    压力测试5: 故障切换 + 持续负载            ║");
        System.out.println("╚══════════════════════════════════════════╝");

        String tableName = "stress_failover_" + totalTableCounter.incrementAndGet();

        Client setupClient = new Client("localhost", 9999, 3);
        String cr = setupClient.execute("CREATE TABLE " + tableName + " (id STRING, info STRING)");
        System.out.println("建表: " + cr);
        Thread.sleep(500);

        // 预插入一些数据
        for (int i = 0; i < 20; i++) {
            setupClient.execute("INSERT INTO " + tableName + " (id, info) VALUES ('" + i + "', 'pre_" + i + "')");
        }
        Thread.sleep(500);

        // 启动持续写入（后台线程，10 个并发客户端各做 30 次操作）
        int loadThreads = 10;
        int opsPerThread = 30;
        int totalOps = loadThreads * opsPerThread;

        System.out.println("启动 " + loadThreads + " 个并发客户端持续写入，每客户端 " + opsPerThread + " 次操作");

        ExecutorService executor = Executors.newFixedThreadPool(loadThreads);
        CountDownLatch latch = new CountDownLatch(loadThreads);
        AtomicInteger beforeFailoverOps = new AtomicInteger(0);
        AtomicInteger afterFailoverOps = new AtomicInteger(0);

        // 用一个标志表示是否已触发故障
        AtomicInteger failoverTriggered = new AtomicInteger(0);

        for (int t = 0; t < loadThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    Client client = new Client("localhost", 9999, 3);
                    client.setRetryIntervalMs(500);
                    for (int i = 0; i < opsPerThread; i++) {
                        String key = "fo_" + threadId + "_" + i;
                        String sql = "INSERT INTO " + tableName + " (id, info) VALUES ('" 
                                + key + "', 'failover_data_" + key + "')";
                        executeSqlWithMetrics(client, sql);

                        if (failoverTriggered.get() == 0) {
                            beforeFailoverOps.incrementAndGet();
                        } else {
                            afterFailoverOps.incrementAndGet();
                        }

                        // 小延迟模拟真实负载
                        Thread.sleep(50);
                    }
                } catch (Exception e) {
                    log.error("Failover 测试线程异常", e);
                } finally {
                    latch.countDown();
                }
            });
        }

        // 等待一些操作先执行
        Thread.sleep(2000);

        // 触发 Master 故障
        System.out.println("\n>>> 触发 Master1 故障！<<<");
        failoverTriggered.set(1);
        master1.stop();
        System.out.println(">>> Master1 已停止，等待 Master2 接管... <<<");

        // 等待选举完成
        waitForCondition(() -> isPortOpen(9998), 5000);
        Thread.sleep(1000);
        System.out.println(">>> Master2 已接管 <<<\n");

        // 等待所有操作完成
        latch.await(120, TimeUnit.SECONDS);
        executor.shutdown();

        System.out.println("故障前完成的操作: " + beforeFailoverOps.get());
        System.out.println("故障后完成的操作: " + afterFailoverOps.get());

        printReport("故障切换 + 持续负载", totalOps, 0);

        // 故障切换场景允许更高的错误率
        double errorRate = (double) errorCount.get() / totalOps;
        assertTrue(errorRate < 0.50, "故障切换下错误率极高: " + String.format("%.2f%%", errorRate * 100));
        System.out.println("✓ 系统在 Master 故障切换期间保持了基本可用性");
    }

    // ======================== 工具方法 ========================

    /**
     * 执行 SQL 并记录延迟和成功/失败
     */
    private void executeSqlWithMetrics(Client client, String sql) {
        long start = System.nanoTime();
        try {
            String result = client.execute(sql);
            long elapsed = (System.nanoTime() - start) / 1_000_000; // ms
            latencies.add(elapsed);

            if (result != null && result.startsWith("Error")) {
                errorCount.incrementAndGet();
            } else {
                successCount.incrementAndGet();
            }
        } catch (Exception e) {
            long elapsed = (System.nanoTime() - start) / 1_000_000;
            latencies.add(elapsed);
            errorCount.incrementAndGet();
        }
    }

    /**
     * 打印性能报告
     */
    private void printReport(String testName, int totalOps, long durationMs) {
        List<Long> sortedLatencies = new ArrayList<>(latencies);
        Collections.sort(sortedLatencies);

        int total = successCount.get() + errorCount.get();
        double errorRate = total > 0 ? (double) errorCount.get() / total * 100 : 0;

        // 计算 TPS（如果 duration > 0）
        double tps = durationMs > 0 ? (double) total / durationMs * 1000 : 0;

        // 计算延迟分位数
        long avgLatency = 0, p50 = 0, p95 = 0, p99 = 0, maxLatency = 0, minLatency = 0;
        if (!sortedLatencies.isEmpty()) {
            long sum = 0;
            for (long l : sortedLatencies) sum += l;
            avgLatency = sum / sortedLatencies.size();
            minLatency = sortedLatencies.get(0);
            maxLatency = sortedLatencies.get(sortedLatencies.size() - 1);
            p50 = sortedLatencies.get((int) (sortedLatencies.size() * 0.50));
            p95 = sortedLatencies.get((int) (sortedLatencies.size() * 0.95));
            p99 = sortedLatencies.get(Math.min((int) (sortedLatencies.size() * 0.99), sortedLatencies.size() - 1));
        }

        System.out.println();
        System.out.println("┌──────────────────────────────────────────────────────┐");
        System.out.println("│          性能报告: " + padRight(testName, 35) + "│");
        System.out.println("├──────────────────────────────────────────────────────┤");
        System.out.println("│  总请求数:    " + padRight(String.valueOf(total), 39) + "│");
        System.out.println("│  成功数:      " + padRight(String.valueOf(successCount.get()), 39) + "│");
        System.out.println("│  失败数:      " + padRight(String.valueOf(errorCount.get()), 39) + "│");
        System.out.println("│  错误率:      " + padRight(String.format("%.2f%%", errorRate), 39) + "│");
        if (durationMs > 0) {
            System.out.println("│  总耗时:      " + padRight(durationMs + " ms", 39) + "│");
            System.out.println("│  TPS:         " + padRight(String.format("%.2f ops/s", tps), 39) + "│");
        }
        System.out.println("├──────────────────────────────────────────────────────┤");
        System.out.println("│  延迟统计 (ms):                                      │");
        System.out.println("│    最小:      " + padRight(minLatency + " ms", 39) + "│");
        System.out.println("│    平均:      " + padRight(avgLatency + " ms", 39) + "│");
        System.out.println("│    P50:       " + padRight(p50 + " ms", 39) + "│");
        System.out.println("│    P95:       " + padRight(p95 + " ms", 39) + "│");
        System.out.println("│    P99:       " + padRight(p99 + " ms", 39) + "│");
        System.out.println("│    最大:      " + padRight(maxLatency + " ms", 39) + "│");
        System.out.println("└──────────────────────────────────────────────────────┘");
    }

    private String padRight(String s, int n) {
        // 考虑中文字符宽度
        int displayWidth = getDisplayWidth(s);
        if (displayWidth >= n) return s;
        StringBuilder sb = new StringBuilder(s);
        for (int i = 0; i < n - displayWidth; i++) {
            sb.append(' ');
        }
        return sb.toString();
    }

    private int getDisplayWidth(String s) {
        int width = 0;
        for (char c : s.toCharArray()) {
            if (Character.UnicodeScript.of(c) == Character.UnicodeScript.HAN) {
                width += 2;
            } else {
                width += 1;
            }
        }
        return width;
    }

    // ======================== 基础设施工具 ========================

    private boolean isPortOpen(int port) {
        try (Socket s = new Socket("localhost", port)) {
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private boolean waitForCondition(Callable<Boolean> condition, int timeoutMs) {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeoutMs) {
            try {
                if (Boolean.TRUE.equals(condition.call())) return true;
            } catch (Exception ignored) {}
            try { Thread.sleep(200); } catch (Exception ignored) {}
        }
        return false;
    }

    private void safeClose(String name, CloseAction action) {
        try {
            action.close();
            System.out.println(name + " 已关闭");
        } catch (Exception e) {
            System.out.println(name + " 关闭异常: " + e.getMessage());
        }
    }

    @FunctionalInterface
    interface CloseAction {
        void close() throws Exception;
    }

    private void cleanupTestData() {
        deleteDirectory(new File("./stress_test_db_1"));
        deleteDirectory(new File("./stress_test_db_2"));
        System.out.println("已清理压力测试数据目录");
    }

    private void deleteDirectory(File dir) {
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) deleteDirectory(file);
                    else file.delete();
                }
            }
            dir.delete();
        }
    }
}
