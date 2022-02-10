package org.example.schema;

import org.apache.flink.table.types.DataType;

/**
 * Class to define column schema of TPS-DS table. The Column schema consists of field name {@link
 * String} and field type {@link DataType}.
 */
public class Column {
    private final String name;
    private final DataType dataType;

    public Column(String name, DataType dataType) {
        this.name = name;
        this.dataType = dataType;
    }

    public String getName() {
        return name;
    }

    public DataType getDataType() {
        return dataType;
    }
}