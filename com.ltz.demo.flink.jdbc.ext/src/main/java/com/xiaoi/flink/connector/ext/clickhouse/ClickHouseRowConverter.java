package com.xiaoi.flink.connector.ext.clickhouse;

import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.types.logical.RowType;

public class ClickHouseRowConverter extends AbstractJdbcRowConverter {

    private static final long serialVersionUID = 1L;

    public ClickHouseRowConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public String converterName() {
        return "ClickHouse";
    }
}
