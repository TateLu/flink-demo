package org.example.async;

/**
 * @program: flink-demo
 * @description:
 * @author: TATE.LU
 * @create: 2024-03-08 18:14
 **/

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;

public class NonAsyncMysqlExample {

    public static void main(String[] args) throws Exception {
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 假设这是我们要查询的键的列表
        String[] queryKeys = {"1", "2", "3","4","5","6","7","8","9","10","11","12"};

        // 创建一个自定义的数据源来从MySQL读取数据
        DataStream<String> mysqlData = env.addSource(new MysqlSourceFunction(queryKeys)).setParallelism(1); // 确保并行度为1

        // 打印结果
        mysqlData.print();

        // 执行任务并记录开始时间
        long startTime = new Date().getTime();
        env.execute("Non-Async MySQL Example");
        // 记录结束时间并计算耗时
        long endTime = new Date().getTime();
        System.out.println("Execution time: " + (endTime - startTime) + " milliseconds");

    }

    // 自定义数据源函数，用于从MySQL读取数据
    public static class MysqlSourceFunction extends RichSourceFunction<String> {
        private transient Connection connection;
        private String[] queryKeys; // 添加查询键数组

        public MysqlSourceFunction(String[] queryKeys) {
            this.queryKeys = queryKeys;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root123");
        }

        @Override
        public void close() throws Exception {
            super.close();
            connection.close();
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            for (String key : queryKeys) {
                PreparedStatement ps = connection.prepareStatement("SELECT * FROM demo WHERE id = ?");
                ps.setString(1, key);
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    String result = rs.getString("name");
                    ctx.collect(result);
                }
            }
        }

        @Override
        public void cancel() {
            // 取消操作
        }
    }
}
