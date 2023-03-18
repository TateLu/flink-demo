package ltz.demo.util;

import ltz.demo.entity.TableFieldDesc;

import java.util.List;
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

}
