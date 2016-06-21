![](../images/DM-logo.jpg)
[![Documentation Status](https://readthedocs.org/projects/streamreactor/badge/?version=latest)](http://streamreactor.readthedocs.io/en/latest/hbase.html#kafka-connect-hbase)

## Kafka Connect Hbase

A Connector and Sink to write events from Kafka to Hbase. The connector takes the value from the Kafka Connect SinkRecords and inserts a new row into Hbase.

When inserting a row in Hbase you need a row key. There are 3 different settings:
*SINK_RECORD - it grabs the SinkRecord.keyValue
*GENERIC - It combines the SinkRecord kafka topic, kafka offset and kafka partition to form the key
*FIELDS - It combines all the specified payload fields to form the row key.

The task expects the table in HBase to exist. By default all the fields in the record would be added to the hbase table and column familty. However you can specify the fields to insert and the column to insert to (see the configuration table below).

## Prerequisites

* Hbase 1.2.0
* Confluent 2.0
* Java 1.8 
* Scala 2.11

## Build

```bash
gradle compile
```

To test

```bash
gradle test
```

To create a fat jar

```bash
gradle fatJar
```

or with no tests run

```
gradle fatJarNoTest
```

You can also use the gradle wrapper

```
./gradlew fatJar
```

See the documentation for more information.