# AWS S3 Connector Source Configuration

Before reading this document it is recommended to be familiar with the [general readme for the S3 Connectors](README-sink.md).

The primary use case of the source is to read in files stored on S3 into Kafka through Kafka Connect.

The source files may have been written by the Lenses.io S3 Sink or other producers.


## Source Configuration

An example configuration is provided:

    name=S3SourceConnectorParquet # this can be anything
    connector.class=io.lenses.streamreactor.connect.aws.s3.source.S3SourceConnector
    tasks.max=1
    connect.s3.kcql=insert into $TOPIC_NAME select * from $BUCKET_NAME:$PREFIX_NAME STOREAS `parquet`
    connect.s3.aws.secret.key=SECRET_KEY
    connect.s3.aws.access.key=ACCESS_KEY
    connect.s3.aws.auth.mode=Credentials
    connect.s3.aws.region=eu-west-1
    value.converter=io.confluent.connect.avro.AvroConverter
    value.converter.schema.registry.url=http://localhost:8089

You should replace $BUCKET_NAME, $PREFIX_NAME and $TOPIC_NAME with the names of the bucket, desired prefix and topic.

Please read below for a detailed explanation of these and other options, including the meaning of WITH_FLUSH_COUNT and its alternatives.


### Auth Mode configuration

2 Authentication modes are available:

#### Credentials

ACCESS_KEY and SECRET_KEY are credentials generated within AWS IAM and must be set and configured with permissions to write to the desired S3 bucket.

    connect.s3.aws.auth.mode=Credentials
    connect.s3.aws.access.key=ACCESS_KEY
    connect.s3.aws.secret.key=SECRET_KEY


#### Default

In this auth mode no credentials need be supplied.  If no auth mode is specified, then this default will be used.

    connect.s3.aws.auth.mode=Default
    
The credentials will be discovered through the default chain, in this order:

> * **Environment Variables** - AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
> * **Java System Properties** - aws.accessKeyId and aws.secretKey
> * **Web Identity Token credentials** from the environment or container
> * **Credential profiles file** at the default location (~/.aws/credentials)
> * **EC2 Credentials** delivered through the Amazon EC2 container service
> * **Instance profile credentials** delivered through the Amazon EC2 metadata service

The full details of the default chain are available on [S3 Documentation](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html)


### Source Format configuration

Format configuration is provided by kcql.

The options for json, avro and parquet will look like the below:

    connect.s3.kcql=insert into $TOPIC_NAME select * from $BUCKET_NAME:$PREFIX_NAME STOREAS `JSON`

The options for the formats are case-insensitive, but they are presented here in the most readable form.

#### JSON Input Format Configuration

Using JSON as an input format allows you to read in files containing JSON content (delimited by new lines), line by line.

    STOREAS `JSON`

Please note: The JSON is not parsed by the S3 Source connector.  There is no difference in handling between Json and Text by the S3 Source connector.

    value.converter=org.apache.kafka.connect.storage.StringConverter

#### Avro Input Format Configuration

Using Avro as the input format allows you to read the Avro-stored messages on S3 back into Kafka's native format.

    STOREAS `Avro`
    
    
It may also be necessary to configure the message converter:

    value.converter=io.confluent.connect.avro.AvroConverter
    value.converter.schema.registry.url=http://localhost:8089 
    
#### Parquet Input Format Configuration

Using Parquet as the input format allows you to read parquet files stored on S3, importing the Avro schemas and values.

    STOREAS `Parquet`
    
It may also be necessary to configure the message converter:

    value.converter=io.confluent.connect.avro.AvroConverter
    value.converter.schema.registry.url=http://localhost:8089 


#### Text Input Format Configuration

If the source files on S3 consist of files containing lines of text, then using the text input format may be desired. 

    STOREAS `Text`
    
It may be required to use the additional configuration options for this connector to ensure that the value is presented as a String.

    value.converter=org.apache.kafka.connect.storage.StringConverter
    

#### CSV Input Format Configuration

This reads CSV files written to S3 into a string to be written back to the Kafka source.
There are 2 options for CSV format, if WithHeaders is included then the first row is skipped.

    STOREAS `CSV_WithHeaders`
    STOREAS `CSV`

Please note there is little distinction between the handling of CSV and handling of TEXT (with the exception that the header row can be skipped).  The CSV is not parsed within the connector.
    
#### Byte(Binary) Input Format Configuration

Bytes can be read back in from S3 and back into message keys/values, depending on how the data was written to the source.

This can be used for reading back in a messages containing binary data that were written out using the s3 source, or alternatively reading binary files to be loaded onto a Kafka queue.

Please see the KCQL options available and the results of these configurations:


| Option                     | KCQL Configuration                   | Records read from file as  |
|----------------------------|--------------------------------------|----------------------------|
| Key and Value (With Sizes) | STOREAS `Bytes_KeyAndValueWithSizes` | Long Long Bytes Bytes      | 
| Key (With Size)            | STOREAS `Bytes_KeyWithSize`          | Long Bytes                 |
| Value (With Size)          | STOREAS `Bytes_ValueWithSize`        | Long Bytes                 |
| Key Only                   | STOREAS `Bytes_KeyOnly`              | Bytes                      |
| Value Only                 | STOREAS `Bytes_ValueOnly`            | Bytes                      |


Using the "With Sizes" options the Source assumes that the files will contain one or two (depending on configuration) 8-byte chunks of data at the start of the file instructing how many bytes to read for the content.

In order to ensure the message is passed through as bytes it may be necessary to set the additional configuration options

    value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
    key.converter=org.apache.kafka.connect.converters.ByteArrayConverter


## Limit Options

To limit the number of file names the source reads from S3 in a single poll, use

    BATCH = 100

In order to limit the number of result rows returned from the source in a single poll operation, you can use the LIMIT clause.

    LIMIT 1000
    

## KCQL Config Options Ordering

The order of the KCQL options is important.  If the correct order is not used then the options will not be recognised.

Often, no error will be presented if this is the case.

The correct order for defining options for the S3 Source is

    BATCH STOREAS LIMIT

An example Kcql string showing most of the available config options for the Sink follows:

    insert into $TopicName select * from $BucketName:$PrefixName BATCH = 500 STOREAS `CSV` LIMIT 100



