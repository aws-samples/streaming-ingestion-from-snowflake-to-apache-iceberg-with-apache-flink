# Streaming Ingestion from Snowflake to Apache Iceberg using Amazon Managed Service for Apache Flink

In this AWS Sample you will deploy using CDK an Amazon Managed Service for Apache Flink Aplication that can consume data from a Snowflake table and write it to Apache Iceberg in an S3 Bucket

## Pre-requisites
As a prerequisite you need to have the following:

- CDK CLI Installed
- CDK Credentials Configured
- Snowflake Cluster
- Snowflake Credentials
- Snowflake Warehouse
- Snowflake Table


## Solution Overview

In the Flink Application, we leverage Flink's parallelism so we are able to query the Snowflake Table in parallel.

As one of the parameters, you will provide a column name from the table, which will be used to shard the table. 

The Apache Flink Application will do the following:
1. First it will query the Table to get the unique values for the column you have provided to be used to shard the table. You will also specify the interval in which we shall query the table to detect new columns
2. With the unique values it will parse them into Tuples of <Column Name,Column Value> and pass it to a Flink Process Function
3. In the process Function, using the Table Name, Shard Column Value and a Sort Column you pass a parameter. It will query the Snowflake table, with a query size of your choosing. To avoid duplicates and do incremental querying. We will store in Flink's State the Row Count returned and use it in next queries as an Offset.
4. In Snowflake, we can use an offset, so when executing a query using a column to order by. We make sure to only return unseen rows from the table.
5. The queries to the table will be executed using Flink's Timers, which we provide the interval as a parameter. 
6. As part of the Flink Application, we will provide an AVDL file with the schema of the table we wish to consume. Using a Maven PlugIn Flink will be able to generate a POJO out of the Avro File. Which we will use to write to the Iceberg Table
7. If the Iceberg Table has not been created, it creates it using the AVRO Schema, and writes the data using Apache Iceberg

## Deployment

### Parameters

For the deployment of this solution you need to provide the following parameters

#### Snowflake
The following parameters are needed for Snowflake configuration

- SnowflakeUser: Specifies the Snowflake user account.
- SnowflakePassword: Specifies the Snowflake user's password.
- SnowflakeSchema: Specifies the Snowflake schema to be used.
- SnowflakeWarehouse: Specifies the Snowflake warehouse to be used.
- SnowflakeDB: Specifies the Snowflake database to be used.
- SnowflakeID: Specifies the Snowflake ID.
- SnowflakePort: Specifies the Snowflake port.
- SnowflakeTable: Specifies the Snowflake table to be used.
- SnowflakeShardColumn: Specifies the Snowflake shard column.
- SnowflakeSortColumn: Specifies the Snowflake sort column.
- SnowflakeColumnInterval: Specifies the interval to retrieve the Snowflake column unique values (Used for Sharding).
- SnowflakeQueryInterval: Specifies the interval for the Snowflake query.
- SnowflakeQuerySize: Specifies the size of the Snowflake query.


#### Iceberg

The following parameters are required to write to Apache Iceberg

- IcebergWarehouse: Specifies the S3 bucket location for the Iceberg warehouse. 
- IcebergDB: Specifies the name of the Glue database for the Iceberg table. 
- IcebergTableName: Specifies the name of the Iceberg table. 
- IcebergPartitionFields: Specifies the partition fields for the Iceberg table. 
- IcebergUpsertFields: Specifies the fields to be used for upserts in the Iceberg table. 
- IcebergOperation: Specifies the operation to be performed on the Iceberg table (e.g., "upsert, append").

You will also need to update the AVDL file in the Apache Flink Application in the src/resources Directory.
Just modify the fields, as if you modify the record name, you will need to update the entire Flink Application

Example AVDL File
```
@namespace("com.amazonaws.services.msf.avro")
protocol SnowTableProtocol {
        record SnowFlakeTable {
                string PRICE;
                string CATEGORY;
                string NAME;
                string UPDATED_AT;
        }
        }
```

With this schema, you could then use *NAME* to be the column to shard the table, *UPDATED_AT* to sort the table when querying

And for Iceberg Table, do remember that if you partition the Table, you need to add the partition field as part of the Upsert Fields if you choose to do Upsert Operation
Be sure to:

### Deployment Steps

1. Git Clone the repository

```shell
git clone <repo>
```

2. CD into repo and cdk folder

```shell
cd streaming-ingestion-from-snowflake-to-apache-iceberg-with-apache-flink/cdk
```

3. Install libraries

```shell
npm install
```

4. CD into the Apache Flink Application folder

```shell
cd flink-snowflake
```

5. Build the Apache Flink Application

```shell
mvn clean package
```

6. Go back to the root folder of the directory

```shell
cd ..
```

7. Bootstrap your AWS environment
```shell
cdk bootstrap
```
8. Deploy the AWS Architecture with the required parameters
```shell
cdk deploy --parameters icebergWarehouse=<IcebergWarehouse> --parameters IcebergDB=<IcebergDB> --parameters IcebergTableName=<IcebergTableName> --parameters IcebergPartitionFields=<IcebergPartitionFields> --parameters IcebergUpsertFields=<IcebergUpsertFields> --parameters IcebergOperation=<IcebergOperation> --parameters SnowflakeUser=<SnowflakeUser> --parameters SnowflakePassword=<SnowflakePassword> --parameters SnowflakeSchema=<SnowflakeSchema> --parameters SnowflakeWarehouse=<SnowflakeWarehouse> --parameters SnowflakeDB=<SnowflakeDB> --parameters SnowflakeID=<SnowflakeID> --parameters SnowflakePort=<SnowflakePort> --parameters SnowflakeTable=<SnowflakeTable> --parameters SnowflakeShardColumn=<SnowflakeShardColumn> --parameters SnowflakeSortColumn=<SnowflakeSortColumn> --parameters SnowflakeColumnInterval=<SnowflakeColumnInterval> --parameters SnowflakeQueryInterval=<SnowflakeQueryInterval> --parameters SnowflakeQuerySize=<SnowflakeQuerySize>
```


After deployment has finished go to AWS Managed Flink Console and Start the application

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

