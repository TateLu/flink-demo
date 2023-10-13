package demo.flink.udf.agg;

import demo.flink.udf.aggregation.ConcatStringInGroup;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

/**
 * @program: flink-demo
 * @description:
 * @author: TATE.LU
 * @create: 2023-10-13 18:26
 **/
public class ConcatStringInGroupTest {

    @Test
    void test(){
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //3 测试自定义函数 ConcatStringInGroup
        tableEnv.createTemporarySystemFunction("ConcatStringInGroup", ConcatStringInGroup.class);

        // create a DataStream
        DataStream<Row> dataStream = env.fromElements(
                Row.of("aaa","male", 12),
                Row.of("aaa", "male",10),
                Row.of("bbb", "female",100),
                Row.of("bbb", "female",120)
        );

        // interpret the insert-only DataStream as a Table
        Table inputTable = tableEnv.fromDataStream(dataStream).as("name", "sex","score");
        inputTable.execute().print();

        inputTable= tableEnv.sqlQuery("select name,ConcatStringInGroup(sex) as `ConcatStringInGroup(sex)`,sum(score) as `max(score)` from "+inputTable +" group by name");
        inputTable.execute().print();
    }
}
