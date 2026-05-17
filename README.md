## maven 的使用

mvn exec:java -Dexec.mainClass="com.yourname.minisql.region.Main"


我们在单机中希望实现的功能
CREATE TABLE();
DROP TAVLE  XXX;
insert
select
delete
update


import com.yourname.minisql.region.manager.DatabaseManager;

public class MiniSQLTestRunner {
    public static void main(String[] args) {
        // 将数据目录设置在项目根目录下的 data 文件夹
        String dataDir = "./data";
        
        try (DatabaseManager db = new DatabaseManager(dataDir)) {
            System.out.println("====== MiniSQL 单机版引擎测试开始 ======\n");

            String[] sqlList = {
                // 1. 测试动态建表 (包含三种不同类型)
                "CREATE TABLE students (id INT, name VARCHAR, score DOUBLE)",
                
                // 2. 测试正常插入数据 (落入 MemTable)
                "INSERT INTO students (id, name, score) VALUES ('1001', 'Alice', '95.5')",
                "INSERT INTO students (id, name, score) VALUES ('1002', 'Bob', '88.0')",
                
                // 3. 测试错误插入：故意不传主键 id (验证刚才修复的防呆机制)
                "INSERT INTO students (name, score) VALUES ('Charlie', '59.9')",
                
                // 4. 测试精准点查
                "SELECT * FROM students WHERE id = '1001'",
                
                // 5. 测试更新数据 (覆盖旧数据)
                "UPDATE students SET score = '100.0' WHERE id = '1001'",
                
                // 6. 验证更新是否成功
                "SELECT * FROM students WHERE id = '1001'",
                
                // 7. 测试删除数据 (插入 Tombstone 墓碑)
                "DELETE FROM students WHERE id = '1002'",
                
                // 8. 验证删除是否生效 (应该查不到 Bob)
                "SELECT * FROM students WHERE id = '1002'"
            };

            for (int i = 0; i < sqlList.length; i++) {
                String sql = sqlList[i];
                System.out.println("👉 [SQL " + (i + 1) + "]: " + sql);
                String result = db.execute(sql);
                System.out.println("✅ [RESULT]: " + result + "\n");
            }

            System.out.println("====== 打印存储引擎底层状态 ======");
            db.printStats();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
## 单机引擎
### 如何保证WAL 与MemTable的一致性问题

### SSTable
id 
index
datapath//indexpath
