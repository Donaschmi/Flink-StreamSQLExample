package org.example.schema;

import org.apache.flink.table.api.DataTypes;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SQLSchemaProvider {
    private static final int sqlTableNums = 2;
    private static final Map<String, SQLSchema> schemaMap = createTableSchemas();
    private static Map<String, SQLSchema> createTableSchemas() {
        final Map<String, SQLSchema> schemaMap = new HashMap<>(sqlTableNums);
        schemaMap.put("table_1",new SQLSchema(Arrays.asList(
                new Column("t1_key", DataTypes.INT()),
                new Column("t1_payload", DataTypes.STRING())
        )));
        schemaMap.put("table_2",new SQLSchema(Arrays.asList(
                new Column("t2_key", DataTypes.INT()),
                new Column("t2_payload", DataTypes.STRING())
        )));
        return schemaMap;
    }
    public static SQLSchema getTableSchema(String tableName) {
        SQLSchema result = schemaMap.get(tableName);
        if (result != null) {
            return result;
        } else {
            throw new IllegalArgumentException(
                    "Table schema of table " + tableName + " does not exist.");
        }
    }
}
