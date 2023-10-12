package demo.flink.udf.udtf;

import cn.hutool.core.util.ArrayUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.junit.jupiter.api.Assertions.*;

class SplitOneColumnToMultiColumnTest {
    @Test
    void test(){
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //3 测试自定义函数 SplitColumns
        final int columnSize = 5;
        tableEnv.registerFunction("SplitOneColumnToMultiColumn", new SplitOneColumnToMultiColumn(columnSize));
        //tableEnv.registerFunction("MyFlatMapFunction", new MyFlatMapFunction());

        // create a DataStream
        DataStream<Row> dataStream = env.fromElements(Row.of("Alice;aaa;123", 12), Row.of("Bob;bbb;123", 10), Row.of("Cary;ccc;123", 100),Row.of("Cary;ccc;123", 120));

        // interpret the insert-only DataStream as a Table
        Table inputTable = tableEnv.fromDataStream(dataStream).as("name", "score");


        String[] names = IntStream.range(0,5).mapToObj(index -> "name"+index).collect(Collectors.toList()).toArray(new String[0]);
        inputTable = inputTable
                .flatMap(call("SplitOneColumnToMultiColumn", $("name"),";")).as(names[0], ArrayUtil.sub(names,1,names.length));

        tableEnv.executeSql("select * from "+inputTable).print();
    }

}