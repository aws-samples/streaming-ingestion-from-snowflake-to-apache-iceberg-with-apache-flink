package com.amazonaws.services.msf;

import com.amazonaws.services.msf.avro.SnowFlakeTable;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.List;

public class SnowflakeToRowDataMapFunction implements MapFunction<SnowFlakeTable, RowData> {

    private final Schema avroSchema; // Avro schema for SnowFlakeTable
    private final RowType rowType; // Flink RowType schema for SnowFlakeTable

    public SnowflakeToRowDataMapFunction(Schema avroSchema, RowType rowType) {
        this.avroSchema = avroSchema;
        this.rowType = rowType;
    }

    @Override
    public RowData map(SnowFlakeTable snowFlakeTable) throws Exception {
        // Create a GenericRowData instance
        GenericRowData rowData = new GenericRowData(avroSchema.getFields().size());

        // Map SnowFlakeTable fields to GenericRowData dynamically using Avro reflection and RowType schema
        List<Schema.Field> avroFields = avroSchema.getFields();
        List<RowType.RowField> rowFields = rowType.getFields();

        for (int i = 0; i < avroFields.size(); i++) {
            Schema.Field avroField = avroFields.get(i);
            RowType.RowField rowField = rowFields.get(i);

            String fieldName = avroField.name();
            Object value = getFieldValue(snowFlakeTable, fieldName); // Get value dynamically

            // Convert value to the appropriate type based on RowType
            Object convertedValue = convertValue(value, rowField.getType());

            // Set the field value in GenericRowData
            rowData.setField(i, convertedValue);
        }

        return rowData;
    }

    private Object getFieldValue(SnowFlakeTable snowFlakeTable, String fieldName) throws Exception {
        // Use reflection to get the value of the field by its name
        Field field = SnowFlakeTable.class.getDeclaredField(fieldName);
        field.setAccessible(true); // Make private fields accessible
        return field.get(snowFlakeTable);
    }

    private Object convertValue(Object value, LogicalType logicalType) {
        if (value == null) {
            return null;
        }

        switch (logicalType.getTypeRoot()) {
            case VARCHAR:
                return StringData.fromString((String) value);
            case INTEGER:
                return (Integer) value;
            case BIGINT:
                return (Long) value;
            case DOUBLE:
                return (Double) value;
            case FLOAT:
                return (Float) value;
            case BOOLEAN:
                return (Boolean) value;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampData.fromInstant((Instant) value);
            // Add more cases as needed
            default:
                throw new UnsupportedOperationException("Unsupported type: " + logicalType);
        }
    }
}
