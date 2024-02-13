# AWS S3 Connector

This is an enterprise Apache Kafka Connector developed by Lenses.io to source & sink to/from AWS S3.  

For full details about features, use cases it supports and how to get enterprise support from Lenses.io visit https://lenses.io/connect/kafka-to-aws-s3/

See a [quick intro](https://vimeo.com/874891392) into how it fits into your Apache Kafka and data architecture. How to use the Connector to [backup/restore](https://vimeo.com/874893270) a Kafka topic when using the Connector through Lenses.io. Or how to [source complex data](https://vimeo.com/876384955) including XML data.

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
