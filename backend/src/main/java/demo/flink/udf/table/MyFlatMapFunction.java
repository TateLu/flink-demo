package demo.flink.udf.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @program: flink-demo
 * @description:
 * @author: TATE.LU
 * @create: 2023-10-12 17:50
 **/
public class MyFlatMapFunction extends TableFunction<Row> {

    public void eval(String str) {
        if (str.contains(";")) {
            String[] array = str.split(";");
            for (int i = 0; i < array.length; ++i) {
                collect(Row.of(array[i], array[i].length()));
            }
        }
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return Types.ROW(Types.STRING, Types.STRING,Types.STRING);
    }
}