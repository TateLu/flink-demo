package demo.flink.entity;

import lombok.Builder;
import lombok.Data;

/**
 * @program: flink-demo
 * @description: flink connector属性
 * @author: TATE.LU
 * @create: 2023-04-27 11:13
 **/
@Data
@Builder
public class FlinkConnectorProperties {
    private String dbUrl;
    private String username;
    private String password;
    private String tableName;
    private String driverClassName;
}
