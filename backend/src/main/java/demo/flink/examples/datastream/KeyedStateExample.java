package demo.flink.examples.datastream;

/**
 * @program: flink-demo
 * @description:
 * @author: TATE.LU
 * @create: 2024-03-19 11:27
 **/

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyedStateExample {


    //实现去重
    public static void main(String[] args) throws Exception {
        // 创建Flink流处理环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 创建一个数据源，这里我们使用一个简单的列表
        DataStreamSource<String> source = env.fromElements("a", "b", "a", "c", "b");
        // 设置同一个分组
        DataStream<String> keyedStream = source.keyBy(f -> 0);
        // 创建一个转换，使用RichFilterFunction来使用keyed state进行去重
        DataStream<String> deduplicatedStream = keyedStream.filter(new RichFilterFunction<String>() {
            // 状态描述符，用于注册和检索状态
            private transient ListState<String> state;
            @Override
            public void open(Configuration parameters) throws Exception {
                // 创建状态描述符，并将其与上下文关联
                ListStateDescriptor<String> descriptor =
                        new ListStateDescriptor<>(
                                "string-list-state",
                                TypeInformation.of(String.class));
                // 从上下文中获取状态
                state = getRuntimeContext().getListState(descriptor);
            }
            @Override
            public boolean filter(String value) throws Exception {
                // 添加新元素到状态
                if (state != null) {
                    // 检查状态中是否已经包含当前值
                    for (String s : state.get()) {
                        if(s.equals(value)){
                            return false;
                        }
                    }
                    state.add(value);
                }

                return true;

            }
            //@Override
            //public void close() throws Exception {
            //    // 在close时清理状态
            //    if (state != null) {
            //        state.clear();
            //    }
            //}
        });
        // 打印结果
        deduplicatedStream.print();
        // 执行环境
        env.execute("Keyed State Deduplication Example");
    }
}
