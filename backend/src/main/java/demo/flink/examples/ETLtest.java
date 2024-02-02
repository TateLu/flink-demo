package demo.flink.examples;

import cn.hutool.core.util.StrUtil;
import demo.flink.entity.TableFieldDesc;
import demo.flink.entity.TableJoinDesc;
import demo.flink.udf.aggregation.ConcatStringInGroup;
import demo.flink.util.FlinkSqlService;
import demo.flink.util.FlinkSqlUtil;
import org.apache.flink.table.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @program: flink-demo-1.16
 * @description:
 * @author: TATE.LU
 * @create: 2023-03-13 18:14
 **/
public class ETLtest {

    public static void main(String[] args) throws Exception {

        // set up the Table API
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        FlinkSqlService flinkSqlService = new FlinkSqlService(tableEnv);
        //init table, 创建两个source table
        Table leftTable = excuteTable_demo_product_data(tableEnv);
        Table rightTable = excuteTable_province_product(tableEnv);
        //join table
        List<List<String>> eqFields = new ArrayList<>(1);
        eqFields.add(Stream.of("province", "shengfen").collect(Collectors.toList()));
        TableJoinDesc tableJoinDesc = TableJoinDesc.builder().leftTable(leftTable).rightTable(rightTable).joinType("inner join").eqFields(eqFields).build();
        Table joinTable = flinkSqlService.joinTwoTable(tableJoinDesc);
        List<String> selectFields = Stream.of("shengfen", "floor_gdp", "avg_lingshoujia", "industry").collect(Collectors.toList());
        Table selectTable = flinkSqlService.selectFields(joinTable, selectFields);
        String view_selectTable = "view_selectTable";
        //table -> sql, 注册表之后，才可以在SQL里使用该表名
        tableEnv.createTemporaryView("view_selectTable", selectTable);
        //selectTable.execute().print();

        List<TableFieldDesc> tableFieldDescs = selectTable.getResolvedSchema().getColumns().stream().map(column -> {
            return TableFieldDesc.builder()
                    .name(column.getName())
                    .type(column.getDataType().toString())
                    .build();
        }).collect(Collectors.toList());

        StatementSet stmtSet = tableEnv.createStatementSet();
        //sink table1
        String province_data_shanxi = "province_data_shanxi";
        flinkSqlService.buildSourceTableFromMysql(province_data_shanxi, tableFieldDescs);
        String sql1 = flinkSqlService.copyTableWithFilterInSql(view_selectTable, tableFieldDescs.stream().map(TableFieldDesc::getName).collect(Collectors.toList()).toArray(new String[0]),
                "industry = '计算机'", province_data_shanxi);
        //sink table2
        String province_data_guangdong = "province_data_guangdong";
        flinkSqlService.buildSourceTableFromMysql(province_data_guangdong, tableFieldDescs);
        String sql2 = flinkSqlService.copyTableWithFilterInSql(view_selectTable, tableFieldDescs.stream().map(TableFieldDesc::getName).collect(Collectors.toList()).toArray(new String[0]),
                "industry = '互联网'", province_data_guangdong);
        //insert 需要批量执行
        stmtSet.addInsertSql(sql1);
        stmtSet.addInsertSql(sql2);
        stmtSet.execute();
    }




    private static Table excuteTable_demo_product_data(TableEnvironment tableEnv) {
        FlinkSqlService flinkSqlService = new FlinkSqlService(tableEnv);
        //create table
        String tableName = "bi_test01";
        List<TableFieldDesc> fields = testTableSql();
        flinkSqlService.buildSourceTableFromMysql(tableName, fields);


        //3 测试自定义函数 ConcatStringInGroup
        tableEnv.createTemporarySystemFunction("ConcatStringInGroup", ConcatStringInGroup.class);

        Table table01 = tableEnv.sqlQuery("select province, ConcatStringInGroup(citychengshi) as concat_citychengshi ,max(riqi),max(guomingdp) "+
                " from " + tableName
                + " group by province ");
        return table01;

    }


    private static Table excuteTable_province_product(TableEnvironment tableEnv) {
        FlinkSqlService flinkSqlService = new FlinkSqlService(tableEnv);
        //create table
        String tableName = "province_product";
        List<TableFieldDesc> fields = listTableFields_province_product();
        flinkSqlService.buildSourceTableFromMysql(tableName, fields);
        Table table01 = tableEnv.sqlQuery(StrUtil.format("select * from {}", tableName));

        // avg(零售价),groupby(省份)
        //List<TableFieldDesc> aggregateColumns = Stream.of(TableFieldDesc.builder().name("lingshoujia").func("avg").alias("avg_lingshoujia").build()).collect(Collectors.toList());
        //List<TableFieldDesc> groupColumns = Stream.of(TableFieldDesc.builder().name("shengfen").func("avg").alias("avg_lingshoujia").build()).collect(Collectors.toList());
        //table01 = flinkSqlUtil.groupBy(table01,new String[]{"avg(lingshoujia)"},new String[]{"shengfen"});
        table01 = tableEnv.sqlQuery(StrUtil.format("select shengfen,avg(lingshoujia) as avg_lingshoujia from {} group by  shengfen", table01));
        //table01.execute().print();
        return table01;
    }

    private static List<TableFieldDesc> testTableSql() {
        final String selectedFields = "province STRING,citychengshi STRING,guomingdp DOUBLE,riqi TIMESTAMP(0),value11 INT";
        return FlinkSqlUtil.parseToTableField(selectedFields);
    }

    private static List<TableFieldDesc> listTableFields_demo_product_data() {
        final String selectedFields = "province STRING,city STRING,gdp DOUBLE,industry STRING";
        return FlinkSqlUtil.parseToTableField(selectedFields);
    }

    private static List<TableFieldDesc> listTableFields_province_product() {
        final String selectedFields = "bianhaoid STRING,shengfen STRING,chengshi STRING,lingshoujia DOUBLE,xiaoshoujine INT";
        return FlinkSqlUtil.parseToTableField(selectedFields);
    }



}
