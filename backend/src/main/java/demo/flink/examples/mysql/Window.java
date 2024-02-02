package demo.flink.examples.mysql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @program: flink-demo
 * @description:
 * @author: TATE.LU
 * @create: 2024-02-02 16:53
 **/
public class Window {
    //create main
    public static void main(String[] args) {
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String ddl = "CREATE TABLE myTable (\n" +
                "  id STRING,\n" +
                "  name STRING,\n" +
                "  start_time TIMESTAMP(0),\n" +
                "  WATERMARK FOR start_time AS start_time\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://localhost:3306/test',\n" +
                "  'username' = 'root',\n" +
                "  'password' = 'root123',\n" +
                "  'table-name' = 'demo'\n" +
                ")";
        tableEnv.executeSql(ddl);
        tableEnv.executeSql("desc myTable").print();
        tableEnv.executeSql("SELECT * FROM TABLE(\n" +
                "   TUMBLE(\n" +
                "     DATA => TABLE myTable,\n" +
                "     TIMECOL => DESCRIPTOR(start_time),\n" +
                "     SIZE => INTERVAL '20' SECONDS))").print();


    }

}
