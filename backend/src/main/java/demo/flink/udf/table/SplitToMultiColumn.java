package demo.flink.udf.table;

import cn.hutool.core.lang.Assert;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @program: flink-demo-1.16
 * @description:
 * @author: TATE.LU
 * @create: 2023-03-13 18:39
 **/
public class SplitToMultiColumn extends TableFunction<Row> {
    private int size;


    /**
     * @param size 要拆成几列
     * */
    public SplitToMultiColumn(int size){
        Assert.isTrue(size > 0,() -> new RuntimeException("列数目必须大于0"));
        this.size = size;
    }

    public void eval(String value, String separator) {
        String[] valueSplits = value.split(separator);
        //
        Row row = new Row(size);
        for (int i = 0; i < size; i++) {
            if (i < valueSplits.length) {
                row.setField(i, valueSplits[i]);
                continue;
            }
            row.setField(i, null);
        }
        //collect 1个row, 是拆成列
        collect(row);

        //collect 多个row, 是拆成行
        //for(int i = 0 ;i < length;i++ ){
        //    row.setField(i,"abc");
        //}
        //collect(row);
    }
    @Override
    public TypeInformation<Row> getResultType() {
        /**
         * 可以通过构造函数传递参数，来判断数据类型
         * */
        TypeInformation<?>[] types = new TypeInformation[size];
        for (int i = 0; i < size; i++) {
            types[i] = Types.STRING;
        }
        return Types.ROW(types);
    }

    //@Override
    //public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    //    DataType[] dataTypes = new DataType[columnSize];
    //    for (int i = 0; i < columnSize; i++) {
    //        dataTypes[i] = typeFactory.createDataType("STRING");
    //    }
    //    return TypeInference.newBuilder().typedArguments(dataTypes).build();
    //    //return TypeInferenceExtractor.forTableFunction(typeFactory, (Class) getClass());
    //}
}
