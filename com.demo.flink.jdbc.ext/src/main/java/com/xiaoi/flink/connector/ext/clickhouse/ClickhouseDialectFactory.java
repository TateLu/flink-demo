package com.xiaoi.flink.connector.ext.clickhouse;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectFactory;

public class ClickhouseDialectFactory implements JdbcDialectFactory {

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:ch:") || url.startsWith("jdbc:clickhouse:") ;
    }

    @Override
    public JdbcDialect create() {
        return new ClickHouseDialect();
    }
}
