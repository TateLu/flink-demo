package demo.flink.examples.window;

/**
 * @program: flink-demo
 * @description:
 * @author: TATE.LU
 * @create: 2024-03-13 14:31
 **/

//public class DataStreamWindow {
//
//
//
//    public static void main(String[] args) throws Exception {
//
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//
//        // 使用模拟数据源
//        //DataStream<Tuple2<String, Long>> userClicksStream = env.addSource(new UserClickSource());
//
//        SingleOutputStreamOperator<Tuple2<String, Long>> stream =env.addSource(new UserClickSource())
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
//                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>()
//                                {
//                                    @Override
//                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp)
//                                    {
//                                        return element.f1;
//                                    }
//                                }));
//
//        // 设置Watermark策略以支持process-time窗口
//        SingleOutputStreamOperator<Double> aggregate = stream
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(2)))
//                .keyBy(0)
//                .window((SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2))))
//                .aggregate(new AvgPv());
//
//        //并行度
//        System.out.println("getParallelism "+aggregate.getParallelism());
//
//        // 打印结果到控制台
//        aggregate.print();
//
//
//        // 执行任务
//        env.execute("Flink Window Example with Simulated Data");
//    }
//
//    public static class AvgPv implements AggregateFunction<Tuple2<String, Long>,
//                Tuple1<HashMap<String,Long>>, Tuple2<String, Long>> {
//        @Override
//        public Tuple1<HashMap<String,Long>> createAccumulator() {
//            // 创建累加器
//            return Tuple1.of(new HashMap<String,Long>());
//        }
//        @Override
//        public Tuple1<HashMap<String,Long>> add(Tuple2<String, Long> value,
//                                                Tuple1<HashMap<String,Long>> accumulator) {
//            // 属于本窗口的数据来一条累加一次，并返回累加器
//            accumulator.f0.putIfAbsent(value.f0, 0L);
//            accumulator.f0.put(value.f0,accumulator.f0.get(value.f0) + 1);
//            return accumulator;
//        }
//        @Override
//        public Tuple2<String, Long> getResult(Tuple1<HashMap<String,Long>> accumulator) {
//            // 窗口闭合时，增量聚合结束，将计算结果发送到下游
//            return (double) accumulator.f1 / accumulator.f0.size();
//        }
//        @Override
//        public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long>
//                                                           a, Tuple2<HashSet<String>, Long> b) {
//            return null;
//        }
//    }
//
//
//
//    public static class UserClickSource implements SourceFunction<Tuple2<String, Long>> {
//        private boolean isRunning = true;
//        private Random rand;
//
//        public UserClickSource() {
//            this.rand = new Random();
//        }
//
//        @Override
//        public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
//            while (isRunning) {
//                // 模拟用户点击事件，每秒生成几个随机用户ID的点击记录
//                String userId = "user_" + rand.nextInt(10);
//                long timestamp = System.currentTimeMillis();
//                ctx.collect(new Tuple2<>(userId, timestamp));
//
//                // 每隔一定时间（例如：100毫秒）生成一次新事件
//                Thread.sleep(rand.nextInt(1000)); // 产生随机延迟，更接近真实场景
//            }
//        }
//
//        @Override
//        public void cancel() {
//            isRunning = false;
//        }
//    }
//
//    // 自定义WindowFunction用于处理时间窗口内的字符串拼接
//    public static class MyStringAggregator implements WindowFunction<
//                Tuple2<String, Long>, // 输入类型
//                String, // 输出类型
//                String, // key类型
//                TimeWindow> { // 窗口类型
//
//        @Override
//        public void apply(String userId, TimeWindow window, Iterable<Tuple2<String, Long>> values, Collector<String> out) {
//            StringBuilder sb = new StringBuilder();
//            for (Tuple2<String, Long> click : values) {
//                sb.append(click.f0).append(",").append(click.f1).append(" # "); // 这里假设click_event是一个字符串
//            }
//            // 移除最后一个逗号和空格
//            out.collect(userId + ": [" + sb.substring(0, sb.length() - 2) + "]");
//        }
//    }
//}