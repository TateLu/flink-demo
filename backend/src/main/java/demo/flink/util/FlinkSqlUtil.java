package demo.flink.util;

import cn.hutool.core.util.StrUtil;
import demo.flink.entity.FlinkConnectorProperties;
import demo.flink.entity.TableFieldDesc;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @program: flink-demo-1.16
 * @description:
 * @author: TATE.LU
 * @create: 2023-03-13 18:14
 **/
public class FlinkSqlUtil {
    public static List<TableFieldDesc> parseToTableField(String ddl) {
        return Stream.of(ddl.split(",")).map(item -> {
            String[] arr = item.split(" ");
            return TableFieldDesc.builder().name(arr[0]).type(arr[1]).build();
        }).collect(Collectors.toList());
    }


    /**
     * 创建表
     * */
    public static String generateFlinkSQLDDL(FlinkConnectorProperties properties, Map<String, String> fieldTypes) {
        StringBuilder ddl = new StringBuilder();

        // Start DDL statement
        ddl.append("CREATE TABLE ").append(properties.getTableName()).append(" (\n");

        // Add fields and their types
        for (Map.Entry<String, String> entry : fieldTypes.entrySet()) {
            ddl.append("  ").append(entry.getKey()).append(" ").append(entry.getValue()).append(",\n");
        }

        // Remove trailing comma and close fields definition
        ddl.setLength(ddl.length() - 2);
        ddl.append("\n) WITH (\n");

        // Add connector properties
        ddl.append("  'connector' = 'jdbc',\n");
        ddl.append("  'url' = '").append(properties.getDbUrl()).append("',\n");
        ddl.append("  'username' = '").append(properties.getUsername()).append("',\n");
        ddl.append("  'password' = '").append(properties.getPassword()).append("',\n");
        ddl.append("  'table-name' = '").append(properties.getTableName()).append("'\n");
        if(StrUtil.isNotBlank(properties.getDriverClassName())){
            ddl.append(",");
            ddl.append("  'driver' = '").append(properties.getDriverClassName()).append("'\n");
        }

        // Close WITH clause
        ddl.append(")");

        return ddl.toString();
    }

}
