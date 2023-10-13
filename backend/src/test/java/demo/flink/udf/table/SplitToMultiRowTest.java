package demo.flink.udf.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @program: flink-demo
 * @description:
 * @author: TATE.LU
 * @create: 2023-10-13 18:28
 **/
public class SplitToMultiRowTest {
    @Test
    void test(){
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //3 测试自定义函数 SplitColumns
        final int columnSize = 5;
        tableEnv.registerFunction("SplitToMultiRow", new SplitToMultiRow(columnSize));

        // create a DataStream
        DataStream<Row> dataStream = env.fromElements(Row.of("Alice;aaa;123", 12), Row.of("Bob;bbb;123", 10), Row.of("Cary;ccc;123", 100),Row.of("Cary;ccc;123", 120));

        // interpret the insert-only DataStream as a Table
        Table inputTable = tableEnv.fromDataStream(dataStream).as("name", "score");


        inputTable = inputTable
                .flatMap(call("SplitToMultiRow", $("name"),";",$("name"),$("score")));

        tableEnv.executeSql("select * from "+inputTable).print();
    }
}
