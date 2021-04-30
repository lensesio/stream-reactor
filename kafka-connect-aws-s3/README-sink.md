# AWS S3 Connector Sink Configuration

Before reading this document it is recommended to be familiar with the [general readme for the S3 Connectors](README-sink.md).

## Sink Configuration

An example configuration is provided:

    name=S3SinkConnectorParquet # this can be anything
    connector.class=io.lenses.streamreactor.connect.aws.s3.sink.S3SinkConnector
    topics=$TOPIC_NAME
    tasks.max=1
    connect.s3.kcql=insert into $BUCKET_NAME:$PREFIX_NAME select * from $TOPIC_NAME STOREAS `parquet` WITH_FLUSH_COUNT = 5000 
    aws.access.key=ACCESS_KEY
    aws.secret.key=SECRET_KEY
    aws.auth.mode=Credentials

You should replace $BUCKET_NAME, $PREFIX_NAME and $TOPIC_NAME with the names of the bucket, desired prefix and topic.

Please read below for a detailed explanation of these and other options, including the meaning of WITH_FLUSH_COUNT and its alternatives.


### Auth Mode configuration

2 Authentication modes are available:

#### Credentials

ACCESS_KEY and SECRET_KEY are credentials generated within AWS IAM and must be set and configured with permissions to write to the desired S3 bucket.

    aws.auth.mode=Credentials
    aws.access.key=ACCESS_KEY
    aws.secret.key=SECRET_KEY


#### Default

In this auth mode no credentials need be supplied.  If no auth mode is specified, then this default will be used.

    aws.auth.mode=Default
    
The credentials will be discovered through the default chain, in this order:

> * **Environment Variables** - AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
> * **Java System Properties** - aws.accessKeyId and aws.secretKey
> * **Web Identity Token credentials** from the environment or container
> * **Credential profiles file** at the default location (~/.aws/credentials)
> * **EC2 Credentials** delivered through the Amazon EC2 container service
> * **Instance profile credentials** delivered through the Amazon EC2 metadata service

The full details of the default chain are available on [S3 Documentation](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html)


### Sink Format configuration

Format configuration is provided by kcql.

The options for json, avro and parquet will look like the below:

    connect.s3.kcql=insert into $BUCKET_NAME:$PREFIX_NAME select * from $TOPIC_NAME STOREAS `JSON`

The options for the formats are case-insensitive, but they are presented here in the most readable form.

#### JSON Output Format Configuration

Using JSON as an output format allows you to convert the complex AVRO structure of the message to a simpler schemaless Json format on output.

    STOREAS `JSON`

#### Avro Output Format Configuration

Using Avro as the output format allows you to output the Avro message.

    STOREAS `Avro`
    
#### Parquet Output Format Configuration

Using Parquet as the output format allows you to output the Avro message to a file readable by a parquet reader, including schemas.

    STOREAS `Parquet`
    
#### Text Output Format Configuration

If the incoming kafka message contains text only and this is to be pushed through to the S3 sink as is, then this option may be desired.

    STOREAS `Text`
    
It may be required to use the additional configuration options for this connector to ensure that the value is presented as a String.

    value.converter=org.apache.kafka.connect.storage.StringConverter
    key.converter=org.apache.kafka.connect.storage.StringConverter
    
#### CSV Output Format Configuration

This converts the fields of the Avro message to string values to be written out to a CSV file.
There are 2 options for CSV format, to write CSV files with the column headers or without.

    STOREAS `CSV_WithHeaders`
    STOREAS `CSV`

#### Byte(Binary) Output Format Configuration

Bytes can be written from the message key and/or the message value depending on which option is configured.

The key only/ value only options can be useful for, as an example, stitching together multiple parts of a binary file.

The content sizes are output first to an (8-byte) long.

In order to ensure the message is passed through as bytes it may be necessary to set the additional configuration options

    value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
    key.converter=org.apache.kafka.connect.converters.ByteArrayConverter
    
Please see the KCQL options available and the results of these configurations:


| Option                     | KCQL Configuration                   | Records written to file as |
|----------------------------|--------------------------------------|------------------------|
| Key and Value (With Sizes) | STOREAS `Bytes_KeyAndValueWithSizes` | Long Long Bytes Bytes      | 
| Key (With Size)            | STOREAS `Bytes_KeyWithSize`          | Long Bytes                 |
| Value (With Size)          | STOREAS `Bytes_ValueWithSize`        | Long Bytes                 |
| Key Only                   | STOREAS `Bytes_KeyOnly`              | Bytes                      |
| Value Only                 | STOREAS `Bytes_ValueOnly`            | Bytes                      |


### Sink Flush configuration

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
    


## Partitioning Options

There are 2 options for grouping (partitioning) the output files.

### Default Partitioning

#### S3 Bucket Layout

In your S3 bucket the sink files will be stored as 

    bucket/prefix/topic/partition/offset.ext
 
Where .ext is the appropriate file extension.

(Please note: the prefix must not be divided into subpaths.)


### Custom Partitioning

#### S3 Bucket Layout

This allows you to store the sink files in your S3 bucket as

    bucket/prefix/customKey1=customValue/topic(partition_offset).ext

or
   
    bucket/prefix/customValue/topic(partition_offset).ext

The custom keys and values can be taken from the kafka message key, from the value record, or the message headers (supporting string-coaxable headers only).

Again, .ext is the appropriate file extension.

(Please note: the prefix must not be divided into subpaths.)



#### Configuring Custom Partitioning

The number of partitions you may configure on your sink is unbounded but bear in mind restrictions on AWS S3 key lengths.

##### Partitions from Message Values

To pull fields from the message values, just use the name of the field from the Avro message.

The fields from the message must be primitive types (string, int, long, etc) in order to partition by them.

Add this to your KCQL string:

    PARTITIONBY fieldA[,fieldB]

##### Partitions from Message Keys

It is possible to partition by the entire message key, as long as the key is coercible into a primitive type:

    PARTITIONBY _key

Where the Kafka message key is not a primitive but a complex Avro object, it is possible to partition by individual fields within the key.

    PARTITIONBY _key.fieldA[, _key.fieldB]

Nested fields are supported, for example

    PARTITIONBY _key.user.profile.subscriptionType

##### Partitions from Message Headers

Kafka message headers may be used for partitioning.  In this case the header must contain a primitive type easily coercible to a String type.

    PARTITIONBY _header.<header_key1>[,_header.<header_key2>]

Where a message header is a Struct or complex type then nested fields are supported.

##### Mixing Partition Types

The above partition types can be mixed to configure advanced partitioning.

For example

    PARTITIONBY fieldA, _key.fieldB, _headers.fieldC

##### Configuring partition display


    WITHPARTITIONER=KeysAndValues
    WITHPARTITIONER=Values 
    
    
## KCQL Config Options Ordering

The order of the KCQL options is important.  If the correct order is not used then the options will not be recognised.

Often, no error will be presented if this is the case.

The correct order for defining parameters for the S3 Sink is

    PARTITIONBY STOREAS WITHPARTITIONER WITH_FLUSH_SIZE WITH_FLUSH_INTERVAL WITH_FLUSH_COUNT

An example Kcql string showing all available config options for the Sink follows:

    insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` WITHPARTITIONER=Values WITH_FLUSH_SIZE=1000 WITH_FLUSH_INTERVAL=200 WITH_FLUSH_COUNT=200


