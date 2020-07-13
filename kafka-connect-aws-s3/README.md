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
* byes
* text

## Building

Build a fat jar in order to contain all the dependencies that are used by kafka connect.

From the root of the stream reactor project issue the command:

    gradle :kafka-connect-aws-s3:shadowJar

## Running (alongside fast-data-dev)

To run fast-data-dev with this connector for demonstration purposes issue this command.

    docker run --rm -it --net=host \
           -v stream-reactor/kafka-connect-aws-s3/build/libs/kafka-connect-aws-s3-2.0.0-2.4.0-all.jar:/connectors/kafka-connect-aws-s3.jar \
           lensesio/fast-data-dev:2.3.0


## Sink Configuration

Please see the [S3 Sink configuration readme](README-sink.md)


## Source Configuration

Please see the [S3 Source configuration readme](README-source.md)

