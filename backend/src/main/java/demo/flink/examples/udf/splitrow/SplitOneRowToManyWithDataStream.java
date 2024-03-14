package demo.flink.examples.udf.splitrow;

import demo.flink.entity.TableFieldDesc;
import demo.flink.util.FlinkSqlService;
import demo.flink.util.FlinkSqlUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.util.List;

/**
 * @program: flink-demo-1.16
 * @description:
 * @author: TATE.LU
 * @create: 2023-03-14 15:20
 **/
public class SplitOneRowToManyWithDataStream {
    public static void main(String[] agrs) throws Exception {
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        FlinkSqlService flinkSqlService = new FlinkSqlService(tableEnv);
        final String selectedFields = "province STRING,citychengshi STRING,guomingdp DOUBLE,riqi TIMESTAMP(0),value11 int";
        List<TableFieldDesc> fields = FlinkSqlUtil.parseToTableField(selectedFields);
        String tableName = "bi_test01";
        //source
        flinkSqlService.buildSourceTableFromMysql(tableName, fields);


        Table inputTable = tableEnv.sqlQuery("select UPPER(province) as province,value11,guomingdp,riqi from " + tableName);
        inputTable.getResolvedSchema().getColumns().forEach(column -> {
            System.out.println("datatype "+column.getDataType().toString());
        });
        //CloseableIterator<Row> rows = inputTable.execute().collect();
        //System.out.println("rowssssss");
        //while(rows.hasNext()){
        //   Row row =  rows.next();
        //   for(int i = 0;i < row.getArity();i++){
        //       System.out.println("field dataType " + row.getField(i).getClass());
        //   }
        //    System.out.println(row);
        //}
        inputTable.execute().print();

        //TODO 日期类型比较特别
        System.out.println("start row transform");
        DataStream<Row> dataStream = tableEnv.toDataStream(inputTable, Row.class);
        String[] fieldNames = {"province", "value11","guomingdp","riqi","province1"};
        TypeInformation[] fieldTypes = {
                TypeInformation.of(String.class),
                TypeInformation.of(Integer.class),
                TypeInformation.of(Double.class),
                TypeInformation.of(LocalDateTime.class),
                TypeInformation.of(String.class)
        };

        final String splitRowFiedName = "province";
        DataStream<Row> newDataStream = dataStream.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public void flatMap(Row row, Collector<Row> collector) throws Exception {
                //拆分为行
                if(row.getField(splitRowFiedName) != null);{
                    String[] values = row.getField(splitRowFiedName).toString().split(";");
                    int newRowLen = 5;
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
        //newDataStream.print();
        env.execute();
        inputTable = tableEnv.fromDataStream(newDataStream, Schema.newBuilder()
                .column("province", "string")
                .column("value11", "int")
                .column("guomingdp", "double")
                .column("riqi", "timestamp(0)")
                .column("province1", "string")
                //.watermark("event_time", "SOURCE_WATERMARK()")
                .build());
        //inputTable = tableEnv.fromDataStream(newDataStream);
        List<Column> columnList= inputTable.getResolvedSchema().getColumns();

        System.out.println("22222 print table");
        tableEnv.executeSql("select * from "+inputTable).print();



    }

}
