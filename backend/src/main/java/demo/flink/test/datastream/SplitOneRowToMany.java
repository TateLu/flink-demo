package demo.flink.test.datastream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @program: flink-demo-1.16
 * @description:
 * @author: TATE.LU
 * @create: 2023-03-14 15:20
 **/
public class SplitOneRowToMany {
    public static void main(String[] agrs) throws Exception {
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create a DataStream
        DataStream<Row> dataStream = env.fromElements(Row.of("Alice;aaa;123", 12), Row.of("Bob;bbb;123", 10), Row.of("Cary;ccc;123", 100),Row.of("Cary;ccc;123", 120));

        // interpret the insert-only DataStream as a Table
        Table inputTable = tableEnv.fromDataStream(dataStream).as("name", "score");
        //List<Column> columnList= inputTable.getResolvedSchema().getColumns();
        inputTable = tableEnv.sqlQuery("select UPPER(name) as name,score from " + inputTable);
        System.out.println("0000000000000 input table ");
        //inputTable.execute().print();

        System.out.println("start row transform");
        dataStream = tableEnv.toDataStream(inputTable, Row.class);
        String[] fieldNames = {"name","score","name1"};
        TypeInformation[] fieldTypes = {TypeInformation.of(String.class),TypeInformation.of(Integer.class),TypeInformation.of(String.class)};
        DataStream<Row> newDataStream = dataStream.flatMap(new FlatMapFunction<Row, Row>() {
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
        }, Types.ROW_NAMED(fieldNames,fieldTypes));



        System.out.println("111111 print datastream");
        newDataStream.print();
        env.execute();
        //inputTable = tableEnv.fromDataStream(dataStream, Schema.newBuilder()
        //        .column("name", "String")
        //        .column("length", "STRING")
        //        .column("name1", "INT")
        //        //.watermark("event_time", "SOURCE_WATERMARK()")
        //        .build());
        inputTable = tableEnv.fromDataStream(newDataStream);
        List<Column> columnList= inputTable.getResolvedSchema().getColumns();

        System.out.println("22222 print table");
        tableEnv.executeSql("select * from "+inputTable).print();



    }

}
