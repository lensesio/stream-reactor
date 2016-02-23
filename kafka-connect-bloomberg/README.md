# Kafka Connect Bloomberg

A Connector to Subscribe to Bloomberg and write to Kafka.

## Perquisites
* Confluent 2.0

## Properties

In addition to the default topics configuration the following options are added:

name | data type | required | description
-----|-----------|----------|------------


Example connector.properties file

```bash 
name=elastic-sink
connector.class=com.datamountaineer.streamreactor.connect.elastic.BloombergSourceConnector
tasks.max=1
topics=test_table
```


## Setup


## Improvements

