# AWS S3 Connector

This is a Kafka Connect connector for AWS S3.

Currently sink is supported (writing from Kafka to S3).

## Supported file formats

* json
* avro
* parquet

## Building

Build a fat jar in order to contain all the dependencies that are used by kafka connect.

From the root of the stream reactor project issue the command:

    gradle :kafka-connect-aws-s3:shadowJar

## Running (alongside fast-data-dev)

To run fast-data-dev with this connector for demonstration purposes issue this command.

    docker run --rm -it --net=host \
           -v stream-reactor/kafka-connect-aws-s3/build/libs/kafka-connect-aws-s3-2.0.0-2.4.0-all.jar:/connectors/kafka-connect-aws-s3.jar \
           lensesio/fast-data-dev:2.3.0


           
## S3 Bucket Layout

In your S3 bucket the sink files will be stored as 

 bucket/prefix/topic/partition/offset
 
The prefix must not be divided into subpaths.


## Configuration

An example configuration is provided:

    name=S3SinkConnectorParquet # this can be anything
    connector.class=io.lenses.streamreactor.connect.aws.s3.sink.S3SinkConnector
    topics=$TOPIC_NAME
    tasks.max=1
    connect.s3.kcql=insert into $BUCKET_NAME:$PREFIX_NAME select * from $TOPIC_NAME STOREAS `json` WITH_FLUSH_COUNT = 5000 
    aws.region=eu-west-1
    aws.access.key=ACCESS_KEY
    aws.secret.key=SECRET_KEY
    aws.auth.mode=Credentials

You should replace $BUCKET_NAME, $PREFIX_NAME and $TOPIC_NAME with the names of the bucket, desired prefix and topic.

ACCESS_KEY and SECRET_KEY are credentials generated within AWS IAM and must be set and configured with permissions to write to the desired S3 bucket.  


### Format configuration

Format configuration is provided by kcql.

The options for json, avro and parquet will look like the below:


    connect.s3.kcql=insert into $BUCKET_NAME:$PREFIX_NAME select * from $TOPIC_NAME STOREAS `json`
    connect.s3.kcql=insert into $BUCKET_NAME:$PREFIX_NAME select * from $TOPIC_NAME STOREAS `avro`
    connect.s3.kcql=insert into $BUCKET_NAME:$PREFIX_NAME select * from $TOPIC_NAME STOREAS `parquet`
    


### Flush configuration

Flush configuration is provided by kcql


#### FLUSH_COUNT

The flush occurs after the configured number of records written to the sink.

For example, if you want a file written for every record, you would set the FLUSH_COUNT to 1.
If you want a file written for each 10,000 records, then you would set the FLUSH_COUNT to 10000
    
    connect.s3.kcql=insert into $BUCKET_NAME:$PREFIX_NAME select * from $TOPIC_NAME STOREAS `json` WITH_FLUSH_COUNT = 1


#### FLUSH_SIZE

The flush occurs after the configured size in bytes is exceeded.

For example, to flush after each 10000 bytes written to a file, you would set the FLUSH_SIZE to 10000.

    connect.s3.kcql=insert into $BUCKET_NAME:$PREFIX_NAME select * from $TOPIC_NAME STOREAS `json` WITH_FLUSH_SIZE = 10000

#### FLUSH_INTERVAL

The flush occurs after the configured interval.

For example, to roll over to a new file after each 10 minutes, you would set the FLUSH_INTERVAL to 600 (10 minutes * 60 seconds)
    
    connect.s3.kcql=insert into $BUCKET_NAME:$PREFIX_NAME select * from $TOPIC_NAME STOREAS `json` WITH_FLUSH_INTERVAL = 600
    
    
