package demo.flink;

/**
 * @program: flink-demo
 * @description:
 * @author: TATE.LU
 * @create: 2024-03-14 10:43
 **/

import org.apache.flink.streaming.api.datastream.DataStream;
        import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
        import org.apache.flink.streaming.api.windowing.time.Time;

public class Demo {
    public static void main(String[] args) throws Exception {
        // 1. 创建Flink执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 创建数据源
        DataStream<Integer> inputStream = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // 3. 定义处理时间窗口，并应用窗口函数
        DataStream<Integer> windowedStream = inputStream
                .timeWindowAll(Time.seconds(5)) // 使用处理时间窗口
                .sum(0); // 应用sum函数来计算每个窗口中的元素数量

        // 4. 打印结果（可选）
        windowedStream.print();

        // 5. 启动执行
        env.execute("Processing Time Window Example");
    }
}
