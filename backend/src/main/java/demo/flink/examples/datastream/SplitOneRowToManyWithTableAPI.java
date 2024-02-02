package demo.flink.examples.datastream;

import demo.flink.entity.TableFieldDesc;
import demo.flink.util.FlinkSqlService;
import demo.flink.udf.MyFlatMapFunction;
import demo.flink.util.FlinkSqlUtil;
import org.apache.flink.api.common.typeinfo.LocalTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @program: flink-demo-1.16
 * @description:
 * @author: TATE.LU
 * @create: 2023-03-14 15:20
 **/
public class SplitOneRowToManyWithTableAPI {
    public static void main(String[] agrs) throws Exception {
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        FlinkSqlService flinkSqlService = new FlinkSqlService(tableEnv);
        final String selectedFields = "province STRING,citychengshi STRING,guomingdp DOUBLE,riqi TIMESTAMP(0),value11 int";
        List<TableFieldDesc> fields = FlinkSqlUtil.parseToTableField(selectedFields);
        String tableName = "bi_test01";
        flinkSqlService.buildSourceTableFromMysql(tableName, fields);


        Table inputTable = tableEnv.sqlQuery("select UPPER(province) as province,value11,guomingdp,riqi from " + tableName);
        inputTable.getResolvedSchema().getColumns().forEach(column -> {
            System.out.println("datatype "+column.getDataType().toString());
        });



        //TODO 日期类型比较特别
        System.out.println("start row transform");
        TypeInformation[] fieldTypes = {
                TypeInformation.of(String.class),
                TypeInformation.of(Integer.class),
                TypeInformation.of(Double.class),
                LocalTimeTypeInfo.LOCAL_DATE_TIME,
                TypeInformation.of(String.class)
        };
        //第一次注册
        System.out.println("separator 1111");
        TableFunction func = new MyFlatMapFunction(";;;",fieldTypes);
        tableEnv.registerFunction("func", func);
        inputTable = inputTable
                .select($("*"))
                .flatMap(call("func", $("province"), $("value11"),$("guomingdp"),$("riqi")))
                .as("province","value11","guomingdp","riqi","separator1");
        tableEnv.createTemporaryView("table01",inputTable);
        inputTable.execute().print();

        //第二次注册
        System.out.println("separator 222");
        TypeInformation[] fieldTypes02 = {
                TypeInformation.of(String.class),
                TypeInformation.of(Integer.class),
                TypeInformation.of(Double.class),
                LocalTimeTypeInfo.LOCAL_DATE_TIME,
                TypeInformation.of(String.class),
                TypeInformation.of(String.class)
        };
        TableFunction newfunc = new MyFlatMapFunction("abc",fieldTypes02);
        tableEnv.registerFunction("newfunc", newfunc);
        Table table01 = tableEnv.from("table01");
        table01 = table01
                .select($("*"))
                .flatMap(call("newfunc", $("province"), $("value11"),$("guomingdp"),$("riqi"),$("separator1")));
                //.as("province","value11","guomingdp","riqi","separator1","separator2");

        table01.execute().print();




    }

}
