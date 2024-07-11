package com.amazonaws.services.msf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

public class ColumnSource implements SourceFunction<Tuple2<String, String>> {
    private volatile boolean isRunning = true;

    private final String url;
    private final String tableName;
    private final String columnName;

    private final int interval;
    private final Properties properties;

    public ColumnSource(String url, String tableName, String columnName, int interval) {
        this.url = url;
        this.tableName = tableName;
        this.columnName = columnName;
        this.interval = interval;

//        // Build the URL
//        String url = String.format("jdbc:snowflake://%s.snowflakecomputing.com:%d/?warehouse=%s&db=%s&schema=%s&user=%s&password=%s",
//                this.snowflakeID, this.port, this.warehouse, this.db, this.schema, this.user, this.password);

        // Initialize the properties
        this.properties = new Properties();
        properties.setProperty("url", url);
        properties.setProperty("drivername", "net.snowflake.client.jdbc.SnowflakeDriver");
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        while (isRunning) {
            try (Connection connection = DriverManager.getConnection(properties.getProperty("url"))) {
                String query = String.format("SELECT DISTINCT %s FROM %s", columnName, tableName);
                try (PreparedStatement statement = connection.prepareStatement(query)) {
                    try (ResultSet resultSet = statement.executeQuery()) {
                        while (isRunning && resultSet.next()) {
                            String uniqueValue = resultSet.getString(1);
                            ctx.collect(new Tuple2<>(columnName, uniqueValue));
                        }
                    }
                }
            }
            Thread.sleep(interval);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
