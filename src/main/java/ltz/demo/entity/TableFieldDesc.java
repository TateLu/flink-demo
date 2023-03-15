package ltz.demo.entity;

import lombok.Builder;
import lombok.Data;

/**
 * @program: flink-demo-1.16
 * @description:
 * @author: TATE.LU
 * @create: 2023-02-09 15:09
 **/
@Data
@Builder
public class TableFieldDesc {
    private String name;
    private String type;
    private String alias;
    private String func;
}
