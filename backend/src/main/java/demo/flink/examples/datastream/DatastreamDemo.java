package demo.flink.examples.datastream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * @program: flink-demo-1.16
 * @description:
 * @author: TATE.LU
 * @create: 2023-03-14 15:20
 **/
public class DatastreamDemo {
    public static void main(String[] agrs) throws Exception {
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create a DataStream
        DataStream<Row> dataStream = env.fromElements(Row.of("Alice;aaa;123", 12), Row.of("Bob;bbb;123", 10), Row.of("Cary;ccc;123", 100));

        // table api
        Table inputTable = tableEnv.fromDataStream(dataStream).as("name", "score");
        inputTable = tableEnv.sqlQuery("select UPPER(name) as name,score from " + inputTable);
        inputTable.execute().print();

        //table to datastream
        dataStream = tableEnv.toDataStream(inputTable);


        dataStream = dataStream.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public void flatMap(Row row, Collector<Row> collector) throws Exception {
                //拆分为行
                if(row.getField("name") != null);{
                    String[] values = row.getField("name").toString().split(";");
                    int newRowLen = 3;
                    for(int i = 0;i < values.length;i++){
                        Row newRow = new Row(newRowLen);
                        int start = 0;
                        while(start < row.getArity()){
                            newRow.setField(start,row.getField(start));
                            start = start + 1;
                        }
                        newRow.setField(start,values[i]);
                        collector.collect(newRow);
                    }


                }
            }
        }).setParallelism(1);


        dataStream.print();
        env.execute();


    }

}
