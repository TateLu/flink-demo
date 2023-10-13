package demo.flink.udf.table;

import cn.hutool.core.lang.Assert;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @program: flink-demo-1.16
 * @description: 字符串列，拆成多行
 * @author: TATE.LU
 * @create: 2023-03-13 18:39
 **/
public class SplitToMultiRow extends TableFunction<Row> {
    private int size;


    /**
     * @param size 要拆成几行
     * */
    public SplitToMultiRow(int size){
        Assert.isTrue(size > 0,() -> new RuntimeException("行数目必须大于0"));
        this.size = size;
    }

    /**
     * @param otherFields 要保留的其它字段
     * */
    public void eval(String value, String separator,Object... otherFields) {
        String[] valueSplits = value.split(separator);
        int length = otherFields.length + 1;
        /**
         * collect 多个row, 是拆成行
         *
         * */
        for (int i = 0; i < size; i++) {
            //此时只有1列
            Row row = new Row(length);
            if (i < valueSplits.length) {
                row.setField(0, valueSplits[i]);
            }else{
                row.setField(0, null);
            }
            for (int j = 1;j < length;j++){
                row.setField(j,otherFields[j - 1]);
            }
            collect(row);
        }


    }
    @Override
    public TypeInformation<Row> getResultType() {
        //对应输出字段的类型
        return Types.ROW(Types.STRING,Types.STRING,Types.INT);
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
