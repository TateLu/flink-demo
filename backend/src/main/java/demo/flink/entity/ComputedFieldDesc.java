package demo.flink.entity;

import lombok.Builder;
import lombok.Data;

/**
 * @program: flink-demo-1.16
 * @description:
 * @author: TATE.LU
 * @create: 2023-02-09 16:39
 **/
@Data
@Builder
public class ComputedFieldDesc {
    private String fieldName;
    private String func;
    private String alias;
}
