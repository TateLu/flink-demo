package demo.flink.util;

import demo.flink.constant.JdbcEnum;
import demo.flink.entity.FlinkConnectorProperties;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class DDLTest {

    @Test
    void generateFlinkSQLDDL_mysql() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String dbUrl = "jdbc:mysql://MyServer:3306/flink_demo";
        String username = "root";
        String password = "root123";
        String tableName = "my_test";
        Map<String, String> fieldTypes = new HashMap<>();
        fieldTypes.put("id", "INT");
        fieldTypes.put("name", "VARCHAR(255)");
        fieldTypes.put("age", "INT");
        FlinkConnectorProperties properties = FlinkConnectorProperties.builder()
                .dbUrl(dbUrl).username(username).password(password).tableName(tableName).build();
        String ddl = FlinkSqlUtil.generateFlinkSQLDDL(properties, fieldTypes);
        System.out.println(ddl);

        tableEnv.executeSql(ddl);
        tableEnv.sqlQuery("select * from my_test").execute().print();
    }

    @Test
    void generateFlinkSQLDDL_clickhouse() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String dbUrl = "jdbc:ch://172.18.208.79:8123/default";
        String username = "default";
        String password = "FGKWThjwCJf2c1us";
        String tableName = "gdp_test";
        String driverClassName = JdbcEnum.clickhouse.getDriverClassName();
        Map<String, String> fieldTypes = new HashMap<>();
        fieldTypes.put("rk", "BIGINT");
        fieldTypes.put("province", "VARCHAR(255)");
        //fieldTypes.put("age", "INT");

        FlinkConnectorProperties properties = FlinkConnectorProperties.builder()
                .dbUrl(dbUrl).username(username).password(password).tableName(tableName).driverClassName(driverClassName)
                .build();
        String ddl = FlinkSqlUtil.generateFlinkSQLDDL(properties, fieldTypes);
        System.out.println(ddl);

        tableEnv.executeSql(ddl);
        tableEnv.sqlQuery("select * from gdp_test").execute().print();
    }
}