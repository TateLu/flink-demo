package demo.flink.entity;

import lombok.Builder;
import lombok.Data;

/**
 * @program: flink-demo-1.16
 * @description: 字段过滤条件
 * @author: TATE.LU
 * @create: 2023-02-09 16:03
 **/
@Data
@Builder
public class FieldFilterDesc {
    private String fieldName;
    private String operator;
    private String value;
}
