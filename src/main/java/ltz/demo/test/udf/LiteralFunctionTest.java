package ltz.demo.test.udf;

import ltz.demo.entity.TableFieldDesc;
import ltz.demo.udf.udtf.LiteralFunction;
import ltz.demo.util.FlinkSqlService;
import ltz.demo.util.FlinkSqlUtil;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import java.util.List;

/**
 * @program: flink-demo-1.16
 * @description:
 * @author: TATE.LU
 * @create: 2023-03-13 19:39
 **/
public class LiteralFunctionTest {
    public static void main(String[] agrs){
        // set up the Table API
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        FlinkSqlService flinkSqlService = new FlinkSqlService(tableEnv);
        //create table
        String tableName = "bi_test01";
        String selectedFields = "province STRING,citychengshi STRING,guomingdp DOUBLE,riqi TIMESTAMP(0),value11 INT,double_value STRING";
        List<TableFieldDesc> fields = FlinkSqlUtil.parseToTableField(selectedFields);
        //create table
        flinkSqlService.createTableInMysql(tableName, fields);

        //3 测试自定义函数 SplitColumns
        tableEnv.createTemporarySystemFunction("PARSE_STRING", LiteralFunction.class);

        Table table01 = tableEnv.sqlQuery("select PARSE_STRING(t1.province,'STRING') as province,PARSE_STRING(t1.double_value,'DOUBLE') as  double_value"+
                " from " + tableName
                + " t1");
        table01.execute().print();
    }


}
