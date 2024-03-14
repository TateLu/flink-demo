package demo.flink.examples.window;

/**
 * @program: flink-demo
 * @description:
 * @author: TATE.LU
 * @create: 2024-03-13 14:31
 **/

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Random;

public class DataStreamWindow {



    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // 使用模拟数据源
        DataStream<Tuple2<String, Long>> userClicksStream = env.addSource(new UserClickSource());

        // 设置Watermark策略以支持process-time窗口
        userClicksStream = userClicksStream
                //.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(2)))
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> v1, Tuple2<String, Long> v2) {

                        //同个key的元素，会一直拼接_啊啊啊
                        return new Tuple2<>(v1.f0+"_啊啊啊", v1.f1);
                    }
                });

        //并行度
        System.out.println("getParallelism "+userClicksStream.getParallelism());

        // 打印结果到控制台
        userClicksStream.print();


        // 执行任务
        env.execute("Flink Window Example with Simulated Data");
    }

    public static class UserClickSource implements SourceFunction<Tuple2<String, Long>> {
        private boolean isRunning = true;
        private Random rand;

        public UserClickSource() {
            this.rand = new Random();
        }

        @Override
        public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
            while (isRunning) {
                // 模拟用户点击事件，每秒生成几个随机用户ID的点击记录
                String userId = "user_" + rand.nextInt(10);
                long timestamp = System.currentTimeMillis();
                ctx.collect(new Tuple2<>(userId, timestamp));

                // 每隔一定时间（例如：100毫秒）生成一次新事件
                Thread.sleep(rand.nextInt(1000)); // 产生随机延迟，更接近真实场景
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    // 自定义WindowFunction用于处理时间窗口内的字符串拼接
    public static class MyStringAggregator implements WindowFunction<
                Tuple2<String, Long>, // 输入类型
                String, // 输出类型
                String, // key类型
                TimeWindow> { // 窗口类型

        @Override
        public void apply(String userId, TimeWindow window, Iterable<Tuple2<String, Long>> values, Collector<String> out) {
            StringBuilder sb = new StringBuilder();
            for (Tuple2<String, Long> click : values) {
                sb.append(click.f0).append(",").append(click.f1).append(" # "); // 这里假设click_event是一个字符串
            }
            // 移除最后一个逗号和空格
            out.collect(userId + ": [" + sb.substring(0, sb.length() - 2) + "]");
        }
    }
}