import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
// import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as assets from 'aws-cdk-lib/aws-s3-assets'
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as glue from 'aws-cdk-lib/aws-glue';
import {CfnParameter, RemovalPolicy} from "aws-cdk-lib";
import * as logs from 'aws-cdk-lib/aws-logs';
import * as kda from 'aws-cdk-lib/aws-kinesisanalyticsv2'

export class CdkSnowflakeStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const IcebergWarehouse = new CfnParameter(this, "icebergWarehouse", {
      type: "String",
      description: "Specifies the S3 bucket location for the Iceberg warehouse"});

    const IcebergDB = new CfnParameter(this, "IcebergDB", {
      type: "String",
      description: "Specifies the name of the Glue database for the Iceberg table."});

    const IcebergTableName = new CfnParameter(this, "IcebergTableName", {
      type: "String",
      description: "Specifies the name of the Iceberg table."});
    const IcebergPartitionFields = new CfnParameter(this, "IcebergPartitionFields", {
      type: "String",
      description: "Specifies the partition fields for the Iceberg table"});
    const IcebergUpsertFields = new CfnParameter(this, "IcebergUpsertFields", {
      type: "String",
      description: "Specifies the fields to be used for upserts in the Iceberg table"});
    const IcebergOperation = new CfnParameter(this, "IcebergOperation", {
      type: "String",
      description: "Specifies the operation to be performed on the Iceberg table (e.g., \"upsert\")"});
    const SnowflakeUser = new CfnParameter(this, "SnowflakeUser", {
      type: "String",
      description: "Specifies the Snowflake user account"});
    const SnowflakePassword = new CfnParameter(this, "SnowflakePassword", {
      type: "String",
      description: "Specifies the Snowflake user's password"});
    const SnowflakeSchema = new CfnParameter(this, "SnowflakeSchema", {
      type: "String",
      description: "Specifies the Snowflake schema to be used"});
    const SnowflakeWarehouse = new CfnParameter(this, "SnowflakeWarehouse", {
      type: "String",
      description: "Specifies the Snowflake warehouse to be used."});
    const SnowflakeDB = new CfnParameter(this, "SnowflakeDB", {
      type: "String",
      description: "Specifies the Snowflake database to be used"});
    const SnowflakeID = new CfnParameter(this, "SnowflakeID", {
      type: "String",
      description: "Specifies the Snowflake ID"});
    const SnowflakePort = new CfnParameter(this, "SnowflakePort", {
      type: "String",
      description: " Specifies the Snowflake port"});
    const SnowflakeTable = new CfnParameter(this, "SnowflakeTable", {
      type: "String",
      description: "Specifies the Snowflake table to be used"});
    const SnowflakeShardColumn = new CfnParameter(this, "SnowflakeShardColumn", {
      type: "String",
      description: "Specifies the Snowflake shard column"});
    const SnowflakeSortColumn = new CfnParameter(this, "SnowflakeSortColumn", {
      type: "String",
      description: "Specifies the Snowflake sort column"});
      const SnowflakeColumnInterval = new CfnParameter(this, "SnowflakeColumnInterval", {
      type: "String",
      description: "Specifies the interval to retrieve the Snowflake column unique values (Used for Sharding)."});
    const SnowflakeQueryInterval = new CfnParameter(this, "SnowflakeQueryInterval", {
      type: "String",
      description: "Specifies the interval for the Snowflake query."});
    const SnowflakeQuerySize = new CfnParameter(this, "SnowflakeQuerySize", {
      type: "String",
      description: "Specifies the size of the Snowflake query"});



    //Jar Connectors
    const flinkApplicationJar = new assets.Asset(this, 'flinkApplicationJar', {
      path: ('../flink-snowflake/target/SnowFlake-1.0.jar'),
    });

    const s3Bucket = new s3.Bucket(this, 'Bucket', {
      bucketName : 'snowflake-iceberg'+this.account+'-'+this.region,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      versioned: true,
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true

    });

    const glueDB = new glue.CfnDatabase(this, "gluedatabase", {
      catalogId: this.account.toString(),
      databaseInput: {
        name: IcebergDB.valueAsString
      }
    });

    const s3Policy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: ['arn:aws:s3:::'+flinkApplicationJar.s3BucketName+'/',
            'arn:aws:s3:::'+flinkApplicationJar.s3BucketName+'/'+flinkApplicationJar.s3ObjectKey],
          actions: ['s3:ListBucket','s3:GetBucketLocation','s3:GetObject','s3:GetObjectVersion']
        }),
      ],
    });

    const s3WritePolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: [s3Bucket.bucketArn,s3Bucket.bucketArn+"/",s3Bucket.bucketArn+"/*"],
          actions: ["s3:*"]
        }),
      ],
    });

    const logGroup = new logs.LogGroup(this, 'MyLogGroup', {
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: RemovalPolicy.DESTROY,
      logGroupName: this.stackName.toLowerCase()+"-log-group"// Adjust retention as needed
    });

    const logStream = new logs.LogStream(this, 'MyLogStream', {
      logGroup,
    });

    const accessCWLogsPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: ["arn:aws:logs:" + this.region + ":" + this.account + ":log-group:"+logGroup.logGroupName,
            "arn:aws:logs:" + this.region + ":" + this.account + ":log-group:"+logGroup.logGroupName+":log-stream:" + logStream.logStreamName],
          actions: ['logs:PutLogEvents','logs:DescribeLogGroups','logs:DescribeLogStreams','cloudwatch:PutMetricData'],
        }),
      ],
    });

    // our KDA app needs access to access glue db
    const glueAccessPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: ['*'],
          actions: ['glue:*']
        }),
      ],
    });

    // our KDA app needs access to describe kinesisanalytics
    const kdaAccessPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: ['*'],
          actions: ['kinesisanalytics:*']
        }),
      ],
    });

    const managedFlinkRole = new iam.Role(this, 'Managed Flink Role', {
      assumedBy: new iam.ServicePrincipal('kinesisanalytics.amazonaws.com'),
      description: 'Managed Flink BedRock Role',
      inlinePolicies: {
        GlueAccessPolicy: glueAccessPolicy,
        KDAAccessPolicy: kdaAccessPolicy,
        AccessCWLogsPolicy: accessCWLogsPolicy,
        S3Policy: s3Policy,
        S3WritePolicy:s3WritePolicy
      },
    });

    const managedFlinkApplication = new kda.CfnApplication(this, 'Managed Flink Application', {
      applicationName: 'flink-snowflake-iceberg',
      runtimeEnvironment: 'FLINK-1_18',
      serviceExecutionRole: managedFlinkRole.roleArn,
      applicationConfiguration: {
        applicationCodeConfiguration: {
          codeContent: {
            s3ContentLocation: {
              bucketArn: 'arn:aws:s3:::'+flinkApplicationJar.s3BucketName,
              fileKey: flinkApplicationJar.s3ObjectKey
            }
          },
          codeContentType: "ZIPFILE"
        },
        applicationSnapshotConfiguration: {
          snapshotsEnabled: true
        },
        environmentProperties: {
          propertyGroups: [{
            propertyGroupId: 'FlinkApplicationProperties',
            propertyMap: {
              'iceberg.warehouse': "s3://"+s3Bucket.bucketName,
              'iceberg.db': IcebergDB.valueAsString,
              'iceberg.table':IcebergTableName.valueAsString,
              'iceberg.partition.fields': IcebergPartitionFields.valueAsString,
              'iceberg.operation': IcebergOperation.valueAsString,
              'iceberg.upsert.equality.fields':IcebergUpsertFields.valueAsString,
              "snowflake.user": SnowflakeUser.valueAsString,
              "snowflake.password": SnowflakePassword.valueAsString,
              "snowflake.schema": SnowflakeSchema.valueAsString,
              "snowflake.warehouse": SnowflakeWarehouse.valueAsString,
              "snowflake.db": SnowflakeDB.valueAsString,
              "snowflake.id": SnowflakeID.valueAsString,
              "snowflake.port": SnowflakePort.valueAsString,
              "snowflake.table.name": SnowflakeTable.valueAsString,
              "snowflake.column.name": SnowflakeShardColumn.valueAsString,
              "snowflake.order.column.name":SnowflakeSortColumn.valueAsString,
              "snowflake.interval":SnowflakeColumnInterval.valueAsString,
              "snowflake.query.interval":SnowflakeQueryInterval.valueAsString,
              "snowflake.query.size.interval":SnowflakeQuerySize.valueAsString



            },
          }],
        },

        flinkApplicationConfiguration: {
          parallelismConfiguration: {
            parallelism: 1,
            configurationType: 'CUSTOM',
            parallelismPerKpu: 1,
            autoScalingEnabled: false

          },
          monitoringConfiguration: {
            configurationType: "CUSTOM",
            metricsLevel: "APPLICATION",
            logLevel: "INFO"
          },
        }

      }
    });


    const cfnApplicationCloudWatchLoggingOption= new kda.CfnApplicationCloudWatchLoggingOption(this,"managedFlinkLogs", {
      applicationName: "flink-snowflake-iceberg",
      cloudWatchLoggingOption: {
        logStreamArn: "arn:aws:logs:" + this.region + ":" + this.account + ":log-group:"+logGroup.logGroupName+":log-stream:" + logStream.logStreamName,
      },
    });

    cfnApplicationCloudWatchLoggingOption.node.addDependency(managedFlinkApplication)


    // The code that defines your stack goes here

    // example resource
    // const queue = new sqs.Queue(this, 'CdkSnowflakeQueue', {
    //   visibilityTimeout: cdk.Duration.seconds(300)
    // });
  }
}
