package ltz.demo.entity;

import lombok.Builder;
import lombok.Data;
import org.apache.flink.table.api.Table;

import java.util.List;

/**
 * @program: flink-demo-1.16
 * @description: 关联2个表
 * @author: TATE.LU
 * @create: 2023-02-09 17:45
 **/
@Data
@Builder
public class TableJoinDesc {
    private Table leftTable;
    private Table rightTable;
    private String joinType;
    //2个表关联相等的字段
    private List<List<String>> eqFields;


}
