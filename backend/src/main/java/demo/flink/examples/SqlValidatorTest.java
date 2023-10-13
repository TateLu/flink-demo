package demo.flink.examples;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Column;

import java.util.List;

/**
 * @program: flink-demo-1.16
 * @description: 创建临时表来测试SQL语法是否正确
 * @author: TATE.LU
 * @create: 2023-03-10 01:39
 **/
public class SqlValidatorTest {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
    TableEnvironment tableEnvironment = TableEnvironment.create(settings);

    public static final SqlValidatorTest instance = new SqlValidatorTest();
    public   synchronized List<Column> testSqlValidate() {
        List<Column> columns = null;
        try {
            tableEnvironment.executeSql("CREATE TEMPORARY TABLE 数据测试1 (\n" +
                    "  name STRING,\n" +
                    "  dept_name STRING,\n" +
                    "  height DOUBLE\n" +
                    ") WITH (\n" +
                    "  'connector' = 'datagen'\n" +
                    ");");

            tableEnvironment.executeSql("CREATE TEMPORARY TABLE dept (\n" +
                    "  name STRING,\n" +
                    "  id DOUBLE\n" +
                    ") WITH (\n" +
                    "  'connector' = 'datagen'\n" +
                    ");");
            Table table = tableEnvironment.sqlQuery("select t1.*,IFNULL(t2.name,'') as name2 from 数据测试1 t1,dept t2 where t1.dept_name = t2.name ");
           columns = table.getResolvedSchema().getColumns();
            columns.forEach(column -> {
                System.out.println(column.toString());
            });

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            tableEnvironment.executeSql("drop TEMPORARY table IF EXISTS 数据测试1");
            tableEnvironment.executeSql("drop TEMPORARY table IF EXISTS dept");
        }

        return columns;


    }

}
