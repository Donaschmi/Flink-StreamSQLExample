package org.example;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.graph.GlobalStreamExchangeMode;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.types.utils.TypeConversions;
import org.example.schema.SQLSchema;
import org.example.schema.SQLSchemaProvider;

import java.util.Arrays;
import java.util.List;

public class StreamSQLTestProgram {

    private static final List<String> SQL_TABLES =
            Arrays.asList(
                    "table_1",
                    "table_2");

    private static final String DATA_SUFFIX = ".dat";
    private static final String COL_DELIMITER = "|";
    private static final String FILE_SEPARATOR = "/";

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String sourceTablePath = params.getRequired("sourceTablePath");
        String sinkTablePath = params.getRequired("sinkTablePath");
        TableEnvironment tableEnvironment = prepareTableEnv(sourceTablePath);
        System.out.println("[INFO]Run SQL query ...");
        String queryString = "SELECT t1_key, t1_payload, t2_payload from table_1 join table_2 on table_1.t1_key = table_2.t2_key LIMIT 1";
        Table resultTable = tableEnvironment.sqlQuery(queryString);
        String sinkTableName = "query_sinkTable";
        ((TableEnvironmentInternal) tableEnvironment)
                .registerTableSinkInternal(
                        sinkTableName,
                        new CsvTableSink(
                                sinkTablePath + FILE_SEPARATOR + "result.ans",
                                COL_DELIMITER,
                                1,
                                FileSystem.WriteMode.OVERWRITE,
                                resultTable.getSchema().getFieldNames(),
                                resultTable.getSchema().getFieldDataTypes()));
        TableResult tableResult = resultTable.executeInsert(sinkTableName);
        // wait job finish
        tableResult.getJobClient().get().getJobExecutionResult().get();
        System.out.println("[INFO]Run SQL query success.");
    }
    /**
     * Prepare TableEnvironment for query.
     *
     * @param sourceTablePath
     * @return
     */
    private static TableEnvironment prepareTableEnv(String sourceTablePath) {
        // init Table Env
        EnvironmentSettings environmentSettings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(environmentSettings);

        // config Optimizer parameters
        tEnv.getConfig()
                .getConfiguration()
                .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
        // TODO use the default shuffle mode of batch runtime mode once FLINK-23470 is implemented
        tEnv.getConfig()
                .getConfiguration()
                .setString(
                        ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE,
                        GlobalStreamExchangeMode.POINTWISE_EDGES_PIPELINED.toString());
        tEnv.getConfig()
                .getConfiguration()
                .setLong(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD,
                        10 * 1024 * 1024);
        tEnv.getConfig()
                .getConfiguration()
                .setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, true);

        // register TPC-DS tables
        SQL_TABLES.forEach(
                table -> {
                    SQLSchema schema = SQLSchemaProvider.getTableSchema(table);
                    CsvTableSource.Builder builder = CsvTableSource.builder();
                    builder.path(sourceTablePath + FILE_SEPARATOR + table + DATA_SUFFIX);
                    for (int i = 0; i < schema.getFieldNames().size(); i++) {
                        builder.field(
                                schema.getFieldNames().get(i),
                                TypeConversions.fromDataTypeToLegacyInfo(
                                        schema.getFieldTypes().get(i)));
                    }
                    builder.fieldDelimiter(COL_DELIMITER);
                    builder.emptyColumnAsNull();
                    builder.lineDelimiter("\n");
                    CsvTableSource tableSource = builder.build();
                    ConnectorCatalogTable catalogTable =
                            ConnectorCatalogTable.source(tableSource, false);
                    tEnv.getCatalog(tEnv.getCurrentCatalog())
                            .ifPresent(
                                    catalog -> {
                                        try {
                                            catalog.createTable(
                                                    new ObjectPath(
                                                            tEnv.getCurrentDatabase(), table),
                                                    catalogTable,
                                                    false);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    });
                });
        return tEnv;
    }
}
