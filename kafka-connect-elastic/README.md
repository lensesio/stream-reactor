# Kafka Connect Elastic

A Connector and Sink to write events from Kafka to Elastic Search using [Elastic4s](https://github.com/sksamuel/elastic4s) client. The connector converts the value from the Kafka Connect SinkRecords to Json and uses Elastic4s's JSON insert functionality to index.

The Sink creates an Index and Type corresponding to the topic name and uses the JSON insert functionality from Elastic4s

## Perquisites
* Confluent 2.0

## Properties

In addition to the default topics configuration the following options are added:

name | data type | required | description
-----|-----------|----------|------------
host_names | string | yes | Host names of elastic search cluster

Example connector.properties file

```bash 
name=elastic-sink
connector.class=com.datamountaineer.streamreactor.connect.elastic.ElasticSinkConnector
host_names=localhost
tasks.max=1
topics=test_table
```


## Setup


## Improvements

