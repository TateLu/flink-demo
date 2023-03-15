package ltz.demo.udf.aggregation;

import org.apache.flink.table.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @program: flink-demo-1.16
 * @description: 分组聚合之后，拼接某个字段的所有值为字符串
 * @author: TATE.LU
 * @create: 2023-02-23 13:25
 **/

public class ConcatStringInGroup extends AggregateFunction<String, ConcatStringInGroupAccmulator> {

    public static final String DELIMITER = "/";

    @Override
    public ConcatStringInGroupAccmulator createAccumulator() {
        return new ConcatStringInGroupAccmulator();
    }

    @Override
    public String getValue(ConcatStringInGroupAccmulator acc) {
        return Optional.ofNullable(acc.value).orElse("");
    }

    public void accumulate(ConcatStringInGroupAccmulator acc, String value) {
        if(acc.value == null){
            acc.value = value;
        }else{
            acc.value = acc.value + DELIMITER + value;
            //字符串去重
            acc.value = Stream.of(acc.value.split(DELIMITER)).distinct().collect(Collectors.joining(DELIMITER));
        }
    }


    public void merge(ConcatStringInGroupAccmulator acc, Iterable<ConcatStringInGroupAccmulator> it) {
        List<String> list = new ArrayList<>();
        for (ConcatStringInGroupAccmulator item : it) {
            list.add(item.value);
        }
        acc.value = list.stream().collect(Collectors.joining(DELIMITER));
    }

}

