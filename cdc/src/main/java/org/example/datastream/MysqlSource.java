package org.example.datastream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.example.constant.ConnectionConst;

import java.sql.*;

public class MysqlSource extends RichParallelSourceFunction<Tuple2<Integer, String>> {

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
                System.out.println(id+":"+name);
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