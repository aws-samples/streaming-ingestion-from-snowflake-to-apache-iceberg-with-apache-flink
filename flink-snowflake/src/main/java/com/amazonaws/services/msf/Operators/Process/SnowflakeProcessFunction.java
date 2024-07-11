package com.amazonaws.services.msf;

import com.amazonaws.services.msf.avro.SnowFlakeTable;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

public class SnowflakeProcessFunction extends KeyedProcessFunction<String, Tuple2<String, String>, SnowFlakeTable> {
    private transient ValueState<String> shardColumnState;
    private transient ValueState<Long> timerState;
    private transient ValueState<Integer> offsetState;
    private final String snowflakeUrl;
    private final String tableName;
    private final String orderByColumn;
    private final int querySize;
    private Schema avroSchema; // Avro schema for SnowTable


    private final long timerInterval;



    public SnowflakeProcessFunction(String snowflakeUrl, String tableName, String orderByColumn, int querySize, long timerInterval) {
        this.snowflakeUrl = snowflakeUrl;
        this.tableName = tableName;
        this.orderByColumn = orderByColumn;
        this.querySize = querySize;
        this.timerInterval = timerInterval;

        avroSchema = ReflectData.get().getSchema(SnowFlakeTable.class);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<String> shardColumnDescriptor = new ValueStateDescriptor<>("shardColumn", TypeInformation.of(String.class));
        shardColumnState = getRuntimeContext().getState(shardColumnDescriptor);

        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>("timerState", TypeInformation.of(Long.class));
        timerState = getRuntimeContext().getState(timerDescriptor);

        ValueStateDescriptor<Integer> offsetDescriptor = new ValueStateDescriptor<>("offsetState", TypeInformation.of(Integer.class));
        offsetState = getRuntimeContext().getState(offsetDescriptor);
    }

    @Override
    public void processElement(Tuple2<String, String> value, Context ctx, Collector<SnowFlakeTable> out) throws Exception {
        // Initialize state on first usage
        if (timerState.value() == null) {
            long timer = ctx.timerService().currentProcessingTime() + timerInterval;
            ctx.timerService().registerProcessingTimeTimer(timer);
            timerState.update(timer);
        }

        if (offsetState.value() == null) {
            offsetState.update(0);
        }

        if (shardColumnState.value() == null) {
            shardColumnState.update(value.f0);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<SnowFlakeTable> out) throws Exception {
        // Reset the timer
        long nextTimer = ctx.timerService().currentProcessingTime() + timerInterval;
        ctx.timerService().registerProcessingTimeTimer(nextTimer);
        timerState.update(nextTimer);

        // Fetch the latest records from Snowflake
        String shardColumn = shardColumnState.value();
        querySnowflake(shardColumn, ctx.getCurrentKey(), offsetState.value(), out);
    }

    private void querySnowflake(String columnName, String columnValue, int offset, Collector<SnowFlakeTable> out) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("url", snowflakeUrl);
        properties.setProperty("drivername", "net.snowflake.client.jdbc.SnowflakeDriver");

        try (Connection connection = DriverManager.getConnection(properties.getProperty("url"))) {
            String query = String.format("SELECT * FROM %s WHERE %s = ? ORDER BY %s LIMIT %d OFFSET %d",
                    tableName, columnName, orderByColumn, querySize, offset);

            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, columnValue);

                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    int rowCount = 0;

                    while (resultSet.next()) {
                        rowCount++;
                        SnowFlakeTable snowflakeTable = mapResultSetToAvro(resultSet);
                        out.collect(snowflakeTable);
                    }

                    // Update the offset
                    offset += rowCount;
                    offsetState.update(offset);

                    // If we fetched less than the query size, reset offset to 0
//                    if (rowCount < querySize) {
//                        offsetState.update(0);
//                    }
                }
            }
        }
    }

    private SnowFlakeTable mapResultSetToAvro(ResultSet resultSet) throws Exception {
        // Create a new instance of SnowTable using Avro reflection
        SnowFlakeTable snowflakeTable = (SnowFlakeTable) Class.forName(avroSchema.getFullName()).newInstance();

        // Map ResultSet columns to SnowTable fields dynamically using Avro reflection
        for (Schema.Field field : avroSchema.getFields()) {
            String fieldName = field.name();
            Object value = resultSet.getObject(fieldName); // Get value dynamically

            // Set the field value using Avro reflection
            snowflakeTable.put(fieldName, value);
        }

        return snowflakeTable;
    }
}
