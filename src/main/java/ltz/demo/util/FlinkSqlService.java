package ltz.demo.util;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import ltz.demo.entity.ComputedFieldDesc;
import ltz.demo.entity.FieldFilterDesc;
import ltz.demo.entity.TableFieldDesc;
import ltz.demo.entity.TableJoinDesc;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @program: flink-demo-1.16
 * @description:
 * @author: TATE.LU
 * @create: 2023-02-09 16:01
 **/
public class FlinkSqlService {
    private TableEnvironment tableEnvironment;
    
    public FlinkSqlService(TableEnvironment tableEnvironment){
        this.tableEnvironment = tableEnvironment;
    }

    /**
     * return filter table
     * */
    public  final Table filterFields(Table table, List<FieldFilterDesc> filters){
        String sql = filters.stream().map(fieldFilterDesc -> {
            if(NumberUtil.isNumber(fieldFilterDesc.getValue())){
                return StrUtil.format("{} {} {}", fieldFilterDesc.getFieldName(), fieldFilterDesc.getOperator(), fieldFilterDesc.getValue());
            }
            return StrUtil.format("{} {} '{}'", fieldFilterDesc.getFieldName(), fieldFilterDesc.getOperator(), fieldFilterDesc.getValue());
        }).collect(Collectors.joining(" OR "));
        return tableEnvironment.sqlQuery(StrUtil.format("select * from {} where {}",table,sql));

    }

    /**
     * return computed sql
     * example:
     *  select avg(age) as avg_age ,* from xxx
     * */
    public  final Table coomputeFields(Table table, List<ComputedFieldDesc> computedFieldDescs){
        String sql = computedFieldDescs.stream().map(fieldDesc -> {
            String str = StrUtil.format("{}({})", fieldDesc.getFunc(), fieldDesc.getFieldName());
            if(StrUtil.isNotBlank(fieldDesc.getAlias())){
                return StrUtil.format("{} as {}",str,fieldDesc.getAlias());
            }
            return str;
        }).collect(Collectors.joining(","));
        return tableEnvironment.sqlQuery(StrUtil.format("select *,{} from {}",sql,table));
    }

    public  final Table groupBy(Table table,String[] aggregateColumns,String[]  groupColumns){
        return tableEnvironment.sqlQuery(StrUtil.format("select {} from {} group by {}",
                String.join(",",ArrayUtil.addAll(groupColumns,aggregateColumns)),
                table,String.join(",",groupColumns)));
    }

    /**
     * example:
     *  select t1.a,t2,b
     *  from xxx as t1 inner join xxx as t2
     *  on t1.c = t2.d
     * */
    public  final Table joinTwoTable(TableJoinDesc tableJoinDesc){
        String conditon = tableJoinDesc.getEqFields().stream().map(fields -> StrUtil.format("t1.{} = t2.{}",fields.get(0),fields.get(1))).collect(Collectors.joining(" and "));
        return tableEnvironment.sqlQuery(StrUtil.format("select * from {} as t1 {} {} as t2 on {}",
                tableJoinDesc.getLeftTable(), tableJoinDesc.getJoinType(), tableJoinDesc.getRightTable(),conditon));

    }


    public  final Table selectFields(Table table,List<String> fields){
        if(CollectionUtil.isEmpty(fields)){
            return tableEnvironment.sqlQuery(StrUtil.format("select * from {}",table));
        }
        String sql = fields.stream().collect(Collectors.joining(","));
        return tableEnvironment.sqlQuery(StrUtil.format("select {} from {}",sql,table));
    }

    public String getMysqlDDL(String tableName, List<TableFieldDesc> fields) {
        String fieldSql = fields.stream().map(field -> StrUtil.format("{} {}", field.getName(), field.getType())).collect(Collectors.joining(","));
        String primaryKey = StrUtil.format("PRIMARY KEY ({})  NOT ENFORCED", fields.stream().map(TableFieldDesc::getName).collect(Collectors.joining(",")));
        fieldSql = fieldSql +","+ primaryKey;
        String ddl = "CREATE TABLE {} ( {} ) WITH (  " +
                "'connector' = 'jdbc',\n" +
                " 'url' = 'jdbc:mysql://172.16.6.229:3306/bi_insight',\n" +
                " 'username' = 'root',\n" +
                " 'password' = 'xiaoi_219_mysql8_pwd',\n" +
                " 'table-name' = '{}'" +
                ")";
        // String ddl = "CREATE TABLE {} ( {} ) WITH (  " +
        //        "'connector' = 'jdbc',\n" +
        //        " 'url' = 'jdbc:mysql://81.68.120.99:3306/dataease',\n" +
        //        " 'username' = 'root',\n" +
        //        " 'password' = 'root123',\n" +
        //        " 'table-name' = '{}'" +
        //        ")";
        String sql = StrUtil.format(ddl, tableName, fieldSql, tableName);
        //System.out.println(sql);
        return sql;
    }



    public TableResult createTableInMysql(String tableName, List<TableFieldDesc> fields) {
        String ddl = getMysqlDDL(tableName,fields);
        return tableEnvironment.executeSql(ddl);
    }


    /**
     * 读取一个表的数据，插入另一个表
     * @param sourceTable 原表
     * @param fieldNames 原表字段
     * @param filter 原表过滤条件
     * @param destTable 插入数据的目的表
     * */
    public TableResult copyTableWithFilter(String sourceTable,String[] fieldNames,String filter,String destTable){

        String sql = StrUtil.format("select {} from {} ",String.join(",",fieldNames),sourceTable);
        if(StrUtil.isNotBlank(filter)){
            sql = sql + " where " + filter;
        }
        //两种方式都可以
        //方式1
        sql = StrUtil.format("insert into {} {} ",
                destTable,
                sql);
        return tableEnvironment.executeSql(sql);
        //方式2
        //System.out.println(sql);
        //Table table = tableEnvironment.sqlQuery(sql);
        //table.execute().print();
        //return table.executeInsert(destTable);



        //return tableEnvironment.executeSql(sql);
    }


    /**
     * 读取一个表的数据，插入另一个表
     * @param sourceTable 原表
     * @param fieldNames 原表字段
     * @param filter 原表过滤条件
     * @param destTable 插入数据的目的表
     * */
    public String copyTableWithFilterInSql(String sourceTable,String[] fieldNames,String filter,String destTable){

        String sql = StrUtil.format("select {} from {} ",String.join(",",fieldNames),sourceTable);
        if(StrUtil.isNotBlank(filter)){
            sql = sql + " where " + filter;
        }
        //两种方式都可以
        //方式1
        sql = StrUtil.format("insert into {} {} ",
                destTable,
                sql);
        return sql;
        //方式2
        //System.out.println(sql);
        //Table table = tableEnvironment.sqlQuery(sql);
        //table.execute().print();
        //return table.executeInsert(destTable);



        //return tableEnvironment.executeSql(sql);
    }
}
