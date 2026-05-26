## maven 的使用

mvn exec:java -Dexec.mainClass="com.yourname.minisql.region.Main"



## 单机引擎
### 如何保证WAL 与MemTable的一致性问题

### SSTable
id 
index
datapath//indexpath


## 自定义网络通信与静态路由
mvn exec:java -Dexec.mainClass="com.yourname.minisql.region.master.MasterServer"
mvn exec:java -Dexec.mainClass="com.yourname.minisql.region.RegionMain" -Dexec.args="8888"
mvn exec:java -Dexec.mainClass="com.yourname.minisql.region.RegionMain" -Dexec.args="8889"
mvn exec:java -Dexec.mainClass="com.yourname.minisql.region.client.Client"

## Zookeeper 集群化与动态上下线
cd /zookeeper
./zookeeper/bin/zkServer.sh start
./zookeeper/bin/zkServer.sh stop
mvn exec:java -Dexec.mainClass="com.yourname.minisql.region.master.MasterServer"
mvn exec:java -Dexec.mainClass="com.yourname.minisql.region.RegionMain" -Dexec.args="8888"
mvn exec:java -Dexec.mainClass="com.yourname.minisql.region.RegionMain" -Dexec.args="8889"

mvn exec:java -Dexec.mainClass="com.yourname.minisql.region.RegionMain" -Dexec.args="8888 127.0.0.1 master"

mvn exec:java -Dexec.mainClass="com.yourname.minisql.region.RegionMain" -Dexec.args="8889 127.0.0.1 slave"

mvn exec:java -Dexec.mainClass="com.yourname.minisql.region.client.Client"
## 测试
HATEST : 测试调度系统的可用性
curator 选举机制来保证master节点的高可用性
loadbalancer机制，轮询机制

Replication--保证数据的一致性: 
master建表插数据，slave节点也要有
连续插入，每次都要有数据
删除后没数据

Regionregistry:测一下region到zookeeper上注册，删除，相互感知

ServiceDiscovery:测试mastera节点与zookeepera间的，获取在线 Region 列表，轮询负载均衡，Region 上下线通知

zkclient与集群交互的能力:CRUD,连接



集成测试:Region 自动注册,请求路由,CRUD(join没有实现)，region上下线后的容灾容错



-- 基础建表（使用默认列）
CREATE TABLE users;

-- 指定列定义
CREATE TABLE students (id STRING, name STRING, age INT, score DOUBLE);

-- 多表创建
CREATE TABLE orders (id STRING, product STRING, quantity INT);
CREATE TABLE products (id STRING, price DOUBLE, stock INT);


-- 插入单条数据
INSERT INTO users (id, name, age) VALUES ('1', 'Alice', '25');

-- 插入多条数据
INSERT INTO students (id, name, age) VALUES ('1', 'Bob', '20');
INSERT INTO students (id, name, age) VALUES ('2', 'Charlie', '22');
INSERT INTO students (id, name, age) VALUES ('3', 'Diana', '21');

-- 插入带浮点数的数据
INSERT INTO products (id, price, stock) VALUES ('p1', '99.99', '100');

-- 点查询（根据主键）
SELECT * FROM users WHERE id = '1';

-- 查询不存在的记录
SELECT * FROM users WHERE id = '999';


-- 更新单列
UPDATE users SET name = 'Alice Updated' WHERE id = '1';

-- 更新多列
UPDATE users SET name = 'Alice New', age = '26' WHERE id = '1';


-- 删除单条
DELETE FROM users WHERE id = '1';

-- 删除不存在的记录
DELETE FROM users WHERE id = '999';
