package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @program: flink-demo
 * @description:
 * @author: TATE.LU
 * @create: 2024-02-04 16:44
 **/
public class CDC {
    //create main function
    public static void main(String[] args) {
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String ddl = "CREATE TABLE myTable (\n" +
                "  id STRING,\n" +
                "  name STRING,\n" +
                "  start_time TIMESTAMP(0)," +
                "   PRIMARY KEY (id) NOT ENFORCED" +
                "     ) WITH (\n" +
                "     'connector' = 'mysql-cdc',\n" +
                "     'hostname' = 'localhost',\n" +
                "     'port' = '3306',\n" +
                "     'username' = 'root',\n" +
                "     'password' = 'root123',\n" +
                "     'database-name' = 'test',\n" +
                "     'table-name' = 'demo');";
        tableEnv.executeSql(ddl);

        ddl = "CREATE TABLE myTable_sink (\n" +
                "  id STRING,\n" +
                "  name STRING,\n" +
                "  start_time TIMESTAMP(0)," +
                "   PRIMARY KEY (id) NOT ENFORCED" +
                "     ) WITH (\n" +
                "     'connector' = 'mysql-cdc',\n" +
                "     'hostname' = 'localhost',\n" +
                "     'port' = '3306',\n" +
                "     'username' = 'root',\n" +
                "     'password' = 'root123',\n" +
                "     'database-name' = 'test',\n" +
                "     'table-name' = 'demo');";
        tableEnv.executeSql(ddl);

        tableEnv.executeSql("select * from myTable").print();

    }
}
