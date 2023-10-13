package demo.flink.examples.datastream;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

import java.time.LocalDate;

/**
 * @program: flink-demo-1.16
 * @description:
 * @author: TATE.LU
 * @create: 2023-03-14 15:15
 **/
public class ToTable2 {
    public static void main(String[] agrs) throws Exception {
        // setup the environment
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        final TableEnvironment env = TableEnvironment.create(settings);

        // create a table with example data
        final Table customers =
                env.fromValues(
                        DataTypes.of("ROW<name STRING, order_date DATE, item_count INT>"),
                        Row.of("Guillermo Smith", LocalDate.parse("2020-12-01"), 3),
                        Row.of("Guillermo Smith", LocalDate.parse("2020-12-05"), 5),
                        Row.of("Valeria Mendoza", LocalDate.parse("2020-03-23"), 4),
                        Row.of("Valeria Mendoza", LocalDate.parse("2020-06-02"), 10),
                        Row.of("Leann Holloway", LocalDate.parse("2020-05-26"), 9),
                        Row.of("Leann Holloway", LocalDate.parse("2020-05-27"), null),
                        Row.of("Brandy Sanders", LocalDate.parse("2020-10-14"), 1),
                        Row.of("John Turner", LocalDate.parse("2020-10-02"), 12),
                        Row.of("Ellen Ortega", LocalDate.parse("2020-06-18"), 100));
        env.createTemporaryView("customers", customers);

        env.executeSql("select * from customers").print();


    }

}
