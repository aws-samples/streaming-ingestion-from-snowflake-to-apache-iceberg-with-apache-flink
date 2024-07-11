package com.amazonaws.services.msf;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergTableManager {

    public static RowType getRowType(org.apache.avro.Schema avroSchema) {
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        return FlinkSchemaUtil.convert(icebergSchema);
    }

    public static void createIcebergTable(org.apache.avro.Schema avroSchema, String icebergWarehouse, String icebergDB, String icebergTableName, String partitionFields) {
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);

        Map<String, String> catalogProperties = getDefaultCatalogProperties(icebergWarehouse);
        CatalogLoader glueCatalogLoader = CatalogLoader.custom(
                "glue",
                catalogProperties,
                new org.apache.hadoop.conf.Configuration(),
                "org.apache.iceberg.aws.glue.GlueCatalog");

        TableIdentifier outputTable = TableIdentifier.of(icebergDB, icebergTableName);
        Catalog catalog = glueCatalogLoader.loadCatalog();

        List<String> partitionFieldList = Arrays.asList(partitionFields.split("\\s*,\\s*"));
        PartitionSpec partitionSpec = getPartitionSpec(icebergSchema, partitionFieldList);

        createTable(catalog, outputTable, icebergSchema, partitionSpec);
    }

    public static void appendIcebergSink(DataStream<RowData> dataStream, org.apache.avro.Schema avroSchema, String icebergBucket, String icebergDB, String icebergTableName, String icebergOperation, String icebergUpsertFields) {
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);

        Map<String, String> catalogProperties = getDefaultCatalogProperties(icebergBucket);
        CatalogLoader glueCatalogLoader = CatalogLoader.custom(
                "glue",
                catalogProperties,
                new org.apache.hadoop.conf.Configuration(),
                "org.apache.iceberg.aws.glue.GlueCatalog");

        TableIdentifier outputTable = TableIdentifier.of(icebergDB, icebergTableName);
        Catalog catalog = glueCatalogLoader.loadCatalog();
        TableLoader tableLoader = TableLoader.fromCatalog(glueCatalogLoader, outputTable);

        IcebergSinkBuilder icebergSinkBuilder = new IcebergSinkBuilder(icebergOperation,icebergUpsertFields ); // Pass default operation type
        FlinkSink.Builder flinkSinkBuilder = icebergSinkBuilder.buildFlinkSinkBuilder(dataStream, catalog, outputTable, tableLoader, icebergSchema, icebergOperation, icebergUpsertFields);

        flinkSinkBuilder.append(); // Finalize the builder and build the sink
    }

    private static Map<String, String> getDefaultCatalogProperties(String warehouse) {
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("type", "iceberg");
        catalogProperties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        catalogProperties.put("warehouse", warehouse);
        catalogProperties.put("impl", "org.apache.iceberg.aws.glue.GlueCatalog");
        return catalogProperties;
    }

    private static PartitionSpec getPartitionSpec(org.apache.iceberg.Schema icebergSchema, List<String> partitionFieldsList) {
        PartitionSpec.Builder partitionBuilder = PartitionSpec.builderFor(icebergSchema);
        for (String partitionField : partitionFieldsList) {
            partitionBuilder.identity(partitionField);
        }
        return partitionBuilder.build();
    }

    private static void createTable(Catalog catalog, TableIdentifier outputTable, org.apache.iceberg.Schema icebergSchema, PartitionSpec partitionSpec) {
        if (!catalog.tableExists(outputTable)) {
            Table icebergTable = catalog.createTable(outputTable, icebergSchema, partitionSpec);
            TableOperations tableOperations = ((BaseTable) icebergTable).operations();
            TableMetadata appendTableMetadata = tableOperations.current();
            tableOperations.commit(appendTableMetadata, appendTableMetadata.upgradeToFormatVersion(2));
        }
    }
}
