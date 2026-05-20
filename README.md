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

## 主从复制与文件传输

## 容错容灾与负载均衡