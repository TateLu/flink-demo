package demo.flink.udf.udtf;

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
public class SplitOneColumnToMultiColumn extends TableFunction<Row> {
    private int columnSize;


    public SplitOneColumnToMultiColumn(int columnSize){
        Assert.isTrue(columnSize > 0,() -> new RuntimeException("列数目必须大于0"));
        this.columnSize = columnSize;
    }

    public void eval(String value, String separator) {
        String[] valueSplits = value.split(separator);
        //
        Row row = new Row(columnSize);
        for (int i = 0; i < columnSize; i++) {
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
        TypeInformation<?>[] types = new TypeInformation[columnSize];
        for (int i = 0; i < columnSize; i++) {
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
