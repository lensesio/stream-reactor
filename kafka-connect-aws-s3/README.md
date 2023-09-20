# AWS S3 Connector

This is a Kafka Connect connector for AWS S3.

This connector supports the following modes:

* Sink (Data flow from Kafka Connect -> S3)
* Source (Data flow from S3 -> Kafka Connect)

## Supported file formats

* json
* avro
* parquet
* csv
* bytes
* text

## Sink Documentation (including configuration)

Please see the [Lenses Stream Reactor S3 Sink Documentation](https://docs.lenses.io/5.3/connectors/sinks/s3sinkconnector/)


## Source Documentation (including configuration)

Please see the [Lenses Stream Reactor S3 Source Documentation](https://docs.lenses.io/5.3/connectors/sources/s3sourceconnector/)


## Building

Build an assembly package in order to contain all the dependencies that are used by the connector.

From the root of the stream reactor project issue the command:

    sbt "project aws-s3" assembly
