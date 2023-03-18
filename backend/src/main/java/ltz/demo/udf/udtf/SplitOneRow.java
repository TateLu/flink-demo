package ltz.demo.udf.udtf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @program: flink-demo-1.16
 * @description:
 * @author: TATE.LU
 * @create: 2023-03-13 18:39
 **/
@FunctionHint(output = @DataTypeHint("ROW<rowRes STRING>"))
public  class SplitOneRow extends TableFunction<Row> {
    final int length = 3;

    public void eval(String value, String seperator) {
        String[] valueSplits = value.split(seperator);
        //一行，两列
        Row row = new Row(length);

        for (int i = 0; i < length; i++) {
            if (i < valueSplits.length) {
                row.setField(0, valueSplits[i]);
                collect(row);
                continue;
            }
            row.setField(0, null);
            collect(row);
        }


        //增加1行
        //for(int i = 0 ;i < length;i++ ){
        //    row.setField(i,"abc");
        //}
        //collect(row);
    }
    //@Override
    //public TypeInformation<Row> getResultType() {
    //    return new RowTypeInfo(Types.STRING,Types.STRING);
    //}
}