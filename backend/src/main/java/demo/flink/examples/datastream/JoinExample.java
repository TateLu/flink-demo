package demo.flink.examples.datastream;

/**
 * @program: flink-demo
 * @description:
 * @author: TATE.LU
 * @create: 2024-03-08 19:39
 **/

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JoinExample {

    public static void main(String[] args) throws Exception {
        // 创建Flink的执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建第一个DataStream
        DataStream<Tuple2<Integer, String>> stream1 = env.fromElements(
                new Tuple2<>(1, "A"),
                new Tuple2<>(2, "B"),
                new Tuple2<>(3, "C")
        );

        // 创建第二个DataStream
        DataStream<Tuple2<Integer, String>> stream2 = env.fromElements(
                new Tuple2<>(1, "X"),
                new Tuple2<>(2, "Y"),
                new Tuple2<>(3, "Z")
        );

        //// 执行join操作
        //DataStream<Tuple2<Integer, String>> joinedStream = stream1
        //        .join(stream2)
        //        .where(new KeySelector<Tuple2<Integer, String>, Integer>() {
        //            @Override
        //            public Integer getKey(Tuple2<Integer, String> tuple) throws Exception {
        //                return tuple.f0;
        //            }
        //        })
        //        .equalTo(new KeySelector<Tuple2<Integer, String>, Integer>() {
        //            @Override
        //            public Integer getKey(Tuple2<Integer, String> tuple) throws Exception {
        //                return tuple.f0;
        //            }
        //        })
        //        .apply(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>>() {
        //            @Override
        //            public Tuple2<Integer, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
        //                return new Tuple2<>(first.f0, first.f1 + "-" + second.f1);
        //            }
        //        });
        //
        //// 打印连接后的结果
        //joinedStream.print();

        // 执行环境
        env.execute("Flink Join Example");
    }
}

