package org.example;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @program: flink-demo
 * @description:
 * @author: TATE.LU
 * @create: 2024-02-04 16:44
 **/
public class CDC_SQL {
    //create main function
    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("test") // set captured database
                .tableList("test.demo") // set captured table
                .username("root")
                .password("root123")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                //.useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,settings);

        // enable checkpoint
        env.enableCheckpointing(3000);

// SQL 写法
        tEnv.executeSql("CREATE TABLE `mysql_source` (\n" +
                " id      STRING,\n" +
                " name    STRING,\n" +
                " PRIMARY KEY(id) NOT ENFORCED )\n" +
                " WITH (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                // 请修改成 Oracle 所在的实际 IP 地址
                "  'hostname' = 'localhost',\n" +
                "  'port' = '3306',\n" +
                "  'username' = 'root',\n" +
                "  'password' = 'root123',\n" +
                "  'database-name' = 'test',\n" +
                //"  'schema-name' = 'flinkuser',\n" +
                "  'table-name' = 'demo'\n" +
                ")");

        // Stream API 写法
        // SourceFunction<String> sourceFunction = OracleSource.<String>builder()
        //         .hostname("xx.xx.xx.xx")
        //         .port(1521)
        //         .database("xe")
        //         .schemaList("flinkuser")
        //         .tableList("flinkuser.test1")
        //         .username("flinkuser")
        //         .password("flinkpw")
        //         .deserializer(new JsonDebeziumDeserializationSchema())
        //         .build();
        // sEnv.addSource(sourceFunction)


        tEnv.executeSql("CREATE TABLE `mysql_sink` (\n" +
                " `name`    VARCHAR,\n" +
                " `total`  BIGINT NOT NULL," +
                "PRIMARY KEY(name) NOT ENFORCED \n" +
                ") " +
                " WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                // 请修改成 Oracle 所在的实际 IP 地址
                "  'url' = 'jdbc:mysql://localhost:3306/test',\n" +
                "  'username' = 'root',\n" +
                "  'password' = 'root123',\n" +
                //"  'schema-name' = 'flinkuser',\n" +
                "  'table-name' = 'demo_sum'\n" +
                ")");

        // 笔者这里只是进行了最简化的数据转移功能，请根据实际业务情况进行开发
        tEnv.executeSql("insert into mysql_sink select name, count(1) as total from mysql_source group by name");

        //env.execute("Print MySQL Snapshot + Binlog");


    }
}
