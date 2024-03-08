package org.example.async;

/**
 * @program: flink-demo
 * @description:
 * @author: TATE.LU
 * @create: 2024-03-08 17:49
 **/

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class AsyncMysqlDatasourceExample {

    public static void main(String[] args) throws Exception {
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建一个虚拟的数据流来模拟数据输入
        DataStream<String> inputStream = env.fromElements("1", "2", "3","4","5","6","7","8","9","10","11","12");

        // 使用异步I/O函数读取MySQL数据
        DataStream<String> mysqlData = AsyncDataStream.unorderedWait(
                inputStream,
                new AsyncMysqlFunction(),
                10000, // 超时时间
                TimeUnit.MILLISECONDS,
                10 // 最大并发请求
        );

        // 打印结果
        mysqlData.print();

        // 执行任务
        env.execute("Async MySQL Example");
    }

    // 异步I/O函数，用于从MySQL读取数据
    public static class AsyncMysqlFunction extends RichAsyncFunction<String, String> {
        private transient HikariDataSource dataSource;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl("jdbc:mysql://localhost:3306/test");
            config.setUsername("root");
            config.setPassword("root123");
            // 设置其他HikariCP配置参数
            config.setMaximumPoolSize(10);
            config.setMinimumIdle(1);
            config.setMaxLifetime(60000); // 连接最大生命周期
            config.setConnectionTimeout(30000); // 连接超时时间
            dataSource = new HikariDataSource(config);
        }
        @Override
        public void close() throws Exception {
            super.close();
            dataSource.close();
        }
        @Override
        public void asyncInvoke(String key, ResultFuture<String> resultFuture) throws Exception {
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement ps = connection.prepareStatement("SELECT * FROM demo WHERE id = ?")) {

                ps.setString(1, key);
                ResultSet rs = ps.executeQuery();
                StringBuilder result = new StringBuilder();
                while (rs.next()) {
                    result.append("name:"+rs.getString("name")).append(",");
                }
                resultFuture.complete(Collections.singletonList(result.toString()));
            }
        }
    }
}
