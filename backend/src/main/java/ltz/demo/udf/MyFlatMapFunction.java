package ltz.demo.udf;

import cn.hutool.core.util.ArrayUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @program: flink-demo-1.16
 * @description:
 * @author: TATE.LU
 * @create: 2023-03-18 16:42
 **/
public class MyFlatMapFunction extends TableFunction<Row> {
    private String separator;
    private TypeInformation[] fieldTypes;
    public MyFlatMapFunction(String separator,TypeInformation[] fieldTypes){
        this.separator = separator;
        this.fieldTypes = fieldTypes;
    }
    public void eval(Object... objects) {
        Object[] objects1 = new Object[objects.length + 1];
        ArrayUtil.copy(objects,objects1,objects.length);
        objects1[objects.length] = separator;

        collect(Row.of(objects1));
        //String str = objects[0].toString();
        //int len = str.length();
        //Object[] objects1 = new Object[1];
        //collect(Row.of(objects));
        //if (str.contains(this.separator)) {
        //    String[] array = str.split(separator);
        //    for (int i = 0; i < array.length; ++i) {
        //        collect(Row.of(array[i], array[i].length()));
        //    }
        //}
    }



    @Override
    public TypeInformation<Row> getResultType() {
        return Types.ROW(fieldTypes);
    }
}