package ltz.demo.test.udf;

import ltz.demo.entity.TableFieldDesc;
import ltz.demo.udf.aggregation.ConcatStringInGroup;
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
 * @create: 2023-03-13 17:19
 **/
public class ConcatStringInGroupTest {
        public static void main(String[] agrs){
            // set up the Table API
            EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
            TableEnvironment tableEnv = TableEnvironment.create(settings);

            FlinkSqlService flinkSqlService = new FlinkSqlService(tableEnv);
            //create table
            String tableName = "bi_test01";
            String selectedFields = "province STRING,citychengshi STRING,guomingdp DOUBLE,riqi TIMESTAMP(0),value11 INT";
            List<TableFieldDesc> fields = FlinkSqlUtil.parseToTableField(selectedFields);
            //create table
            flinkSqlService.createSourceTableFromMysql(tableName, fields);

            //3 测试自定义函数 ConcatStringInGroup
            tableEnv.createTemporarySystemFunction("ConcatStringInGroup", ConcatStringInGroup.class);

            Table table01 = tableEnv.sqlQuery("select province as province,avg(value11) as avg_value,max(riqi) as max_riqi,max(guomingdp)  as max_gdp,avg(guomingdp) as avg_gdp "+
                    " from " + tableName
                    + " group by province ");
            table01.execute().print();
        }


}
