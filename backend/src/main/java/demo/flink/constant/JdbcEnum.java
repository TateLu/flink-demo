package demo.flink.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum JdbcEnum {
    mysql("mysql","com.mysql.jdbc.Driver"),
    oracle("oracle","oracle.jdbc.driver.OracleDriver"),
    clickhouse("clickhouse","com.clickhouse.jdbc.ClickHouseDriver");

    private String name;
    private String driverClassName;
}
