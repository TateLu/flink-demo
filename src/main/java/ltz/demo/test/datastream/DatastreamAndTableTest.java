package ltz.demo.test.datastream;

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
public class DatastreamAndTableTest {
    public static void main(String[] agrs) throws Exception {
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create a DataStream
        DataStream<Row> dataStream = env.fromElements(Row.of("Alice;aaa;123", 12), Row.of("Bob;bbb;123", 10), Row.of("Cary;ccc;123", 100));

        // interpret the insert-only DataStream as a Table
        Table inputTable = tableEnv.fromDataStream(dataStream).as("name", "score");
        inputTable = tableEnv.sqlQuery("select UPPER(name) as name,score from " + inputTable);
        inputTable.execute().print();

        System.out.println("start row transform");
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
        System.out.println("22222222222 output");
        dataStream.print();
        env.execute();




        // register the Table object as a view and query it
        // the query contains an aggregation that produces updates
        //tableEnv.createTemporaryView("InputTable", inputTable);
        //Table resultTable = tableEnv.sqlQuery("SELECT name, SUM(score) FROM InputTable GROUP BY name");

        // interpret the updating Table as a changelog DataStream
        //DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
        //Table table = tableEnv.fromDataStream(resultStream);
        //table.printSchema();
        //table.execute().print();
        // add a printing sink and execute in DataStream API
        //        resultStream.print();
        //env.execute();

        // prints:
        // +I[Alice, 12]
        // +I[Bob, 10]
        // -U[Alice, 12]
        // +U[Alice, 112]

    }

}
