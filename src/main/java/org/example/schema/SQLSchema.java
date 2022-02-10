package org.example.schema;

import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.stream.Collectors;

/** Class to define table schema of TPS-DS table. The schema consists of a {@link Column} List. */
public class SQLSchema {
    private final List<Column> columns;

    public SQLSchema(List<Column> columns) {
        this.columns = columns;
    }

    public List<String> getFieldNames() {
        return columns.stream().map(column -> column.getName()).collect(Collectors.toList());
    }

    public List<DataType> getFieldTypes() {
        return columns.stream().map(column -> column.getDataType()).collect(Collectors.toList());
    }
}