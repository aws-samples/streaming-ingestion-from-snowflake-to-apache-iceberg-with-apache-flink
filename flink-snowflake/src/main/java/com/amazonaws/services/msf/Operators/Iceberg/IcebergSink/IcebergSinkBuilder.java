package com.amazonaws.services.msf;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.util.Arrays;
import java.util.List;

public class IcebergSinkBuilder {

    private final String DEFAULT_ICEBERG_OPERATION;
    private final String DEFAULT_ICEBERG_UPSERT_FIELDS;

    public IcebergSinkBuilder(String defaultOperation, String defaultIcebergUpsertFields) {
        this.DEFAULT_ICEBERG_OPERATION = defaultOperation;
        this.DEFAULT_ICEBERG_UPSERT_FIELDS = defaultIcebergUpsertFields;
    }

    public FlinkSink.Builder buildFlinkSinkBuilder(DataStream<RowData> dataStream,
                                                   Catalog catalog,
                                                   TableIdentifier outputTable,
                                                   TableLoader tableLoader,
                                                   org.apache.iceberg.Schema icebergSchema,
                                                   String icebergOperation,
                                                   String icebergUpsertFields) {

        FlinkSink.Builder flinkSinkBuilder = FlinkSink
                .forRowData(dataStream)
                .table(catalog.loadTable(outputTable))
                .tableLoader(tableLoader);

        if ("upsert".equals(icebergOperation)) {
            List<String> equalityFieldsList = Arrays.asList(icebergUpsertFields.split("[, ]+"));
            flinkSinkBuilder.equalityFieldColumns(equalityFieldsList).upsert(true);
        } else if ("overwrite".equals(icebergOperation)) {
            flinkSinkBuilder.overwrite(true);
        } else {
            throw new IllegalArgumentException("Unsupported iceberg operation: " + icebergOperation);
        }

        return flinkSinkBuilder;
    }
}
