# Kafka Connect Kudu

A Connector and Sink to write events from Kafka to Kudu. 

## Perquisites
* Confluent 2.0
* Kudu 0.6

## Properties

In addition to the default topics configuration the following options are added:

name | data type | required | description
-----|-----------|----------|------------
kudu.master | string | yes | Kudu master host and port


Example connector.properties file

```bash 
name=elastic-sink
connector.class=com.datamountaineer.streamreactor.connect.kudu.KuduSinkConnector
kudu.master=localhost
tasks.max=1
topics=test_table
```


## Setup


## Improvements

