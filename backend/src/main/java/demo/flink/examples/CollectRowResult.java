package demo.flink.examples;

import cn.hutool.core.date.DateUtil;
import demo.flink.entity.TableFieldDesc;
import demo.flink.util.FlinkSqlService;
import demo.flink.util.FlinkSqlUtil;
import demo.flink.FlinkConstant;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @program: flink-demo-1.16
 * @description:
 * @author: TATE.LU
 * @create: 2023-03-13 18:25
 **/
public class CollectRowResult {

    public static void main(String[] args) throws Exception {

        // set up the Table API
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //init table
        Table leftTable = excuteTable_demo_product_data(tableEnv);

    }




    private static Table excuteTable_demo_product_data(TableEnvironment tableEnv) {
        FlinkSqlService flinkSqlService = new FlinkSqlService(tableEnv);
        //create table
        String tableName = "bi_test01";
        //List<TableFieldDesc> fields = listTableFields_demo_product_data();
        List<TableFieldDesc> fields = testTableSql();
        flinkSqlService.buildSourceTableFromMysql(tableName, fields);


        TableResult tableResult =  tableEnv.sqlQuery("select * from "+ tableName).execute();
        List<Column> columns = tableResult.getResolvedSchema().getColumns();
        System.out.println("=========columns");


        System.out.println("=========rows");
        List<Map<String,String>> fieldList = columns.stream().map(column -> {
            Map<String,String> map = new HashMap<>();
            map.put("fieldName",column.getName());
            map.put("fieldType",resolveBIFieldType(column.getDataType().getConversionClass()));
            return map;
        }).collect(Collectors.toList());

        List<String> columnNames = columns.stream().map(column -> column.getName()).collect(Collectors.toList());
        List<Map<String,String>> values = new ArrayList<>();
        CloseableIterator<Row> it = tableResult.collect();
        for (CloseableIterator<Row> iter = it; iter.hasNext(); ) {
            Row row = iter.next();
            RowKind rowKind = row.getKind();
            switch (rowKind){
                case UPDATE_BEFORE:
                    break;
                case DELETE:
                    values.remove(values.size() - 1);
                    break;
                case INSERT:
                case UPDATE_AFTER:
                    Map<String,String> map = new HashMap<>();
                    for(String name:columnNames){
                        Object object = row.getField(name);
                        if(object instanceof LocalDateTime){
                            //TODO: 需要格式化日期类型？
                            map.put(name, DateUtil.format((LocalDateTime)object,FlinkConstant.DATE_FORMAT));
                        }else{
                            map.put(name,object.toString());
                        }

                    }
                    if(RowKind.INSERT.equals(rowKind)){
                        values.add(map);
                    }else{
                        values.set(Math.max(values.size() - 1,0),map);
                    }
                    break;
                default:
                    break;
            }

            //System.out.println(row.getKind().name()+ " "+row.toString() + row.getField(0) + " " + row.getField(1));

        }


        return null;
    }

    /**
     * 解析 flink -> bi业务 数据类型映射
     * */
    private static String resolveBIFieldType(Class<?> fieldClass) {
        String returnClass = FlinkConstant.TYPE_MAPPING.get(fieldClass);
        return returnClass != null ? returnClass : "string";
    }



    private static List<TableFieldDesc> testTableSql() {
        final String selectedFields = "province STRING,citychengshi STRING,guomingdp DOUBLE,riqi TIMESTAMP(0),value11 INT";
        return FlinkSqlUtil.parseToTableField(selectedFields);
    }



}
