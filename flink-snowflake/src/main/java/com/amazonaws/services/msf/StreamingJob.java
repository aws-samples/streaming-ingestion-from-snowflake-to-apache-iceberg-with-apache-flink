package com.amazonaws.services.msf;

import com.amazonaws.services.msf.avro.SnowFlakeTable;
import org.apache.avro.reflect.ReflectData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import static com.amazonaws.services.msf.ParameterToolLoader.loadApplicationParameters;

public class StreamingJob {

    private static final String DEFAULT_ICEBERG_S3_BUCKET = "";
    private static final String DEFAULT_GLUE_DB = "";
    private static final String DEFAULT_ICEBERG_TABLE_NAME = "";
    private static final String DEFAULT_ICEBERG_PARTITION_FIELDS = "";
    private static final String DEFAULT_ICEBERG_OPERATION = "";
    private static final String DEFAULT_ICEBERG_UPSERT_FIELDS = "";

    private static final String DEFAULT_SNOWFLAKE_USER = "";
    private static final String DEFAULT_SNOWFLAKE_PASSWORD = "";
    private static final String DEFAULT_SNOWFLAKE_SCHEMA = "";
    private static final String DEFAULT_SNOWFLAKE_WAREHOUSE = "";
    private static final String DEFAULT_SNOWFLAKE_DB = "";
    private static final String DEFAULT_SNOWFLAKE_ID = "";
    private static final String DEFAULT_SNOWFLAKE_PORT = "";
    private static final String DEFAULT_SNOWFLAKE_TABLE_NAME = "";
    private static final String DEFAULT_SNOWFLAKE_COLUMN_NAME = "";

    private static final String DEFAULT_SNOWFLAKE_ORDER_COLUMN_NAME = "";

    private static final String DEFAULT_SNOWFLAKE_INTERVAL = "";

    private static final String DEFAULT_SNOWFLAKE_QUERY_INTERVAL = "";

    private static final String DEFAULT_SNOWFLAKE_QUERY_SIZE = "";



    /**
     * Get configuration properties from Amazon Managed Service for Apache Flink runtime properties
     * GroupID "FlinkApplicationProperties", or from command line parameters when running locally
     */


    public static void main(String[] args) throws Exception {
        org.apache.avro.Schema schema = ReflectData.get().getSchema(SnowFlakeTable.class);
        RowType rowType = com.amazonaws.services.msf.IcebergTableManager.getRowType(schema);

        Configuration config = new Configuration();
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool applicationProperties = loadApplicationParameters(args, env);

        if (env instanceof LocalStreamEnvironment) {
            env.enableCheckpointing(10000);
            env.setParallelism(16);
        }

        String SFUser = applicationProperties.get("snowflake.user", DEFAULT_SNOWFLAKE_USER);
        String SFPassword =         applicationProperties.get("snowflake.password", DEFAULT_SNOWFLAKE_PASSWORD);
        String SFSchema =         applicationProperties.get("snowflake.schema", DEFAULT_SNOWFLAKE_SCHEMA);
        String SFWarehouse =         applicationProperties.get("snowflake.warehouse", DEFAULT_SNOWFLAKE_WAREHOUSE);
        String SFDB =         applicationProperties.get("snowflake.db", DEFAULT_SNOWFLAKE_DB);
        String SFID =         applicationProperties.get("snowflake.id", DEFAULT_SNOWFLAKE_ID);
        int SFPort =         Integer.parseInt(applicationProperties.get("snowflake.port", DEFAULT_SNOWFLAKE_PORT));
        String SFTableName =         applicationProperties.get("snowflake.table.name", DEFAULT_SNOWFLAKE_TABLE_NAME);
        String SFColumnName =         applicationProperties.get("snowflake.column.name", DEFAULT_SNOWFLAKE_COLUMN_NAME);
        String SFOrderColumnName =         applicationProperties.get("snowflake.order.column.name", DEFAULT_SNOWFLAKE_ORDER_COLUMN_NAME);

        String icebergBucket = applicationProperties.get("iceberg.warehouse",DEFAULT_ICEBERG_S3_BUCKET);
        String icebergDB = applicationProperties.get("iceberg.db",DEFAULT_GLUE_DB);
        String icebergTableName =        applicationProperties.get("iceberg.table",DEFAULT_ICEBERG_TABLE_NAME);
        String partitionFields = applicationProperties.get("iceberg.partition.fields",DEFAULT_ICEBERG_PARTITION_FIELDS);
        String icebergOperation = applicationProperties.get("iceberg.operation",DEFAULT_ICEBERG_OPERATION);
        String equalityFieldsString = applicationProperties.get("iceberg.upsert.equality.fields",DEFAULT_ICEBERG_UPSERT_FIELDS);


        int SFInterval =         Integer.parseInt(applicationProperties.get("snowflake.interval", DEFAULT_SNOWFLAKE_INTERVAL));

        int SFQueryInterval =         Integer.parseInt(applicationProperties.get("snowflake.query.interval", DEFAULT_SNOWFLAKE_QUERY_INTERVAL));

        int SFQuerySize =          Integer.parseInt(applicationProperties.get("snowflake.query.size.interval", DEFAULT_SNOWFLAKE_QUERY_SIZE));

        // Build the URL
        String url = String.format("jdbc:snowflake://%s.snowflakecomputing.com:%d/?warehouse=%s&db=%s&schema=%s&user=%s&password=%s",
                SFID, SFPort, SFWarehouse, SFDB, SFSchema, SFUser, SFPassword);

        DataStream<Tuple2<String, String>> customSourceStream = env.addSource(new com.amazonaws.services.msf.ColumnSource(url,
                SFTableName,
                SFColumnName,
                SFInterval));

        DataStream<SnowFlakeTable> rowDataStream = customSourceStream
                .keyBy(value -> value.f1)
                .process(new com.amazonaws.services.msf.SnowflakeProcessFunction(url,
                        SFTableName, SFOrderColumnName, SFQuerySize, SFQueryInterval));

        DataStream<RowData> genericRecordDataStream = rowDataStream.map(new com.amazonaws.services.msf.SnowflakeToRowDataMapFunction(schema, rowType));

        com.amazonaws.services.msf.IcebergTableManager.createIcebergTable(
                schema,
                icebergBucket,
                icebergDB,
                icebergTableName,
                partitionFields);

////
//        // Sink to Iceberg Table
        com.amazonaws.services.msf.IcebergTableManager.appendIcebergSink(
                genericRecordDataStream,
                schema,
                icebergBucket,
                icebergDB,
                icebergTableName,
                icebergOperation,
                equalityFieldsString
        );

        env.execute("SnowFlake Job");
    }
}
