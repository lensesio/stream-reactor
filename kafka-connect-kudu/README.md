# Kafka Connect Kudu

A Connector and Sink to write events from Kafka to Kudu. 

## Perquisites
* Confluent 2.0

## Properties

In addition to the default topics configuration the following options are added:

name | data type | required | description
-----|-----------|----------|------------


Example connector.properties file

```bash 
name=elastic-sink
connector.class=com.datamountaineer.streamreactor.connect.elastic.KuduSinkConnector
host_names=localhost
client_mode=local
tasks.max=1
topics=test_table
```


## Setup


## Improvements

