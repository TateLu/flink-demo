package org.example.datastream;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;
import org.example.constant.ConnectionConst;

import java.sql.*;

/**
 * @program: flink-demo
 * @description:
 * @author: TATE.LU
 * @create: 2024-03-11 17:09
 **/
public class JdbcSourceSink {
    public static void main(String[] args) throws Exception {
        //1 内置source，推荐写法
        source();

        //2 自定义source
        //customSource();
    }


    private static void source() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername(ConnectionConst.Mysql.DRIVER_CLASS_NAME)
                .setDBUrl(ConnectionConst.Mysql.URL)
                .setUsername(ConnectionConst.Mysql.USERNAME)
                .setPassword(ConnectionConst.Mysql.PASSWORD)
                .setQuery("SELECT id, name FROM demo")
                .setRowTypeInfo(new RowTypeInfo(Types.STRING, Types.STRING))
                .finish();

        DataStream<Row> stream = env.createInput(jdbcInputFormat);

        // 假设对数据进行一些处理（例如：转换或过滤）
        DataStream<Row> processedStream = stream.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) throws Exception {
                // 这里可以添加你的处理逻辑，这里只是简单地返回原值
                return value;
            }
        });

        // 配置MySQL Sink连接选项
        JdbcConnectionOptions jdbcSinkOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(ConnectionConst.Mysql.URL)
                .withDriverName(ConnectionConst.Mysql.DRIVER_CLASS_NAME)
                .withUsername(ConnectionConst.Mysql.USERNAME)
                .withPassword(ConnectionConst.Mysql.PASSWORD)
                .build();

        // 将处理后的DataStream写入MySQL数据库
        processedStream.addSink(JdbcSink.sink(
                "insert into demo (id, name) values (?, ?)",
                (statement, row) -> {
                    statement.setString(1, ((String)row.getField(0))+ "_"+RandomStringUtils.randomAlphabetic(3));
                    statement.setString(2, (String)row.getField(1)+"_");
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                jdbcSinkOptions
        ));

        // 执行任务
        env.execute("Flink MySQL Read and Write Example");
    }


    private static void customSource() throws Exception {
        // 创建流处理环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建并添加自定义的MySQL源
        DataStream<Tuple2<Integer, String>> sourceStream = env.addSource(new MysqlCustomSource());

        // 添加简单的map操作以验证数据读取（例如：打印）
        DataStream<String> resultStream = sourceStream.map(new MapFunction<Tuple2<Integer, String>, String>() {
            @Override
            public String map(Tuple2<Integer, String> value) throws Exception {
                return "ID: " + value.f0 + ", Name: " + value.f1;
            }
        });

        // 打印结果到控制台
        resultStream.print();

        // 执行任务
        env.execute("MySQL Source Demo");
    }



    // 在实际项目中，你还需要添加错误处理和检查点机制以保证容错性和 Exactly-Once 语义
    public static class MysqlCustomSource extends RichParallelSourceFunction<Tuple2<Integer, String>> {

        private static final long serialVersionUID = 1L;

        private transient Connection connection;
        private transient Statement statement;
        private transient ResultSet resultSet;

        private final String url = ConnectionConst.Mysql.URL;
        private final String username = ConnectionConst.Mysql.USERNAME;
        private final String password = ConnectionConst.Mysql.PASSWORD;
        private final String query = "SELECT id, name FROM demo";

        private volatile boolean isRunning = true;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            // 加载并初始化MySQL驱动
            Class.forName(ConnectionConst.Mysql.DRIVER_CLASS_NAME);

            // 建立到MySQL的连接
            connection = DriverManager.getConnection(url, username, password);
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
        }

        @Override
        public void run(SourceContext<Tuple2<Integer, String>> ctx) throws Exception {
            while (isRunning && resultSet.next()) {
                int id = resultSet.getInt(1);
                String name = resultSet.getString(2);

                synchronized (ctx.getCheckpointLock()) {
                    // 发送数据到Flink流处理系统
                    ctx.collect(new Tuple2<>(id, name));
                }
            }
        }

        @Override
        public void cancel() {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                // 处理关闭资源时可能发生的异常
                e.printStackTrace();
            }
            isRunning = false;
        }

        // 在实际项目中，你还需要添加错误处理和检查点机制以保证容错性和 Exactly-Once 语义
    }

}
