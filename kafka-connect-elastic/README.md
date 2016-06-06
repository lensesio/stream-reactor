![](../images/DM-logo.jpg)
[![Documentation Status](http://docs.datamountaineer.com/en/latest/badge/?version=latest)](http://docs.datamountaineer.com/en/latest/elastic.html)

# Kafka Connect Elastic

A Connector and Sink to write events from Kafka to Elastic Search using [Elastic4s](https://github.com/sksamuel/elastic4s) client. The connector converts the value from the Kafka Connect SinkRecords to Json and uses Elastic4s's JSON insert functionality to index.

The Sink creates an Index and Type corresponding to the topic name and uses the JSON insert functionality from Elastic4s

**Please go to the documentation for Usage.**

## Perquisites
* Confluent 2.0
* Elastic Search 2.2
* Java 1.8 
* Scala 2.11

To build

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

# or with no tests run

gradle fatJarNoTest
```


## Properties

In addition to the default topics configuration the following options are added:

name | data type | required | description
-----|-----------|----------|------------
connect.elastic.url | string | yes | Url include port (default 9300) of a node in the Elastic Cluster.
connect.elastic.cluster.name | string | yes | Elastic cluster name to connect to. 

Example connector.properties file

```bash 
name=elastic-sink
connector.class=com.datamountaineer.streamreactor.connect.elastic.ElasticSinkConnector
connect.elastic.url=localhost:9300
connect.elastic.cluster.name=elasticsearch
tasks.max=1
topics=test_table
```


## Setup

* [Download and install Elastic Search](http://cassandra.apache.org/)

```
curl -L -O https://download.elastic.co/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/2.2.0/elasticsearch-2.2.0.tar.gz
tar -xvf elasticsearch-2.2.0.tar.gz
cd elasticsearch-2.2.0/bin
```

* Start Elastic Search

```
# start with cluster name matching cluster name in connect properties file
./elasticsearch --cluster.name elasticsearch
```

* [Download and install Confluent](http://www.confluent.io/)
* Copy the Elastic sink jar from your build location to `$CONFLUENT_HOME/share/java/kafka-connect-elastic`

```bash
mkdir $CONFLUENT_HOME/share/java/kafka-connect-elastic
cp target/kafka-connect-elastic-0.1-jar-with-dependencies.jar  $CONFLUENT_HOME/share/java/kafka-connect-elastic/
```

* Start Confluents Zookeeper, Kafka and Schema registry

```bash
nohup $CONFLUENT_HOME/bin/zookeeper-server-start $CONFLUENT_HOME/etc/kafka/zookeeper.properties &
nohup $CONFLUENT_HOME/bin/kafka-server-start $CONFLUENT_HOME/etc/kafka/server.properties &
nohup $CONFLUENT_HOME/bin/schema-registry-start $CONFLUENT_HOME/etc/schema-registry/schema-registry.properties &"
```
    
* Start Kafka Connect with the Elastic sink

Elastic Search is on Netty 3.10 so to avoid conflicts we need our Elastic Sink Connector first in the classpath

```bash
export CLASSPATH=$CONFLUENT_HOME/share/java/kafka-connect-elastic/kafka-connect-elastic-0.1-jar-with-dependencies.jar
```

```bash
$CONFLUENT_HOME/bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties etc/kafka-connect-elastic/elastic.properties
```    

* Test with avro console, start the console to create the topic and write values

```bash
$CONFLUENT_HOME/bin/kafka-avro-console-producer \
--broker-list localhost:9092 --topic test_table \
--property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"id","type":"int"}, {"name":"random_field", "type": "string"}]}'
```
    
```bash
#insert at prompt
{"id": 999, "random_field": "foo"}
{"id": 888, "random_field": "bar"}
```
    
   * Query Elastic
    
```
curl -XGET 'http://localhost:9200/test_table/_search?q=id:999'
```

## Distributed Deployment
    
Kafka Connect is intended to be run as a service. A number of nodes and join together to form a 'leaderless' cluster. Each node or worker in
the cluster is also running a REST API to allow submitting, stopping and viewing running tasks.

To start in distributed mode run the following (note we only pass in one properties file):

```bash
export CLASSPATH=kafka-connect-elastic-0.1-jar-with-dependencies.jar
$CONFLUENT_HOME/bin/connect-distributed etc/schema-registry/connect-avro-distributed.properties
```

Now you can post in your task configuration

```bash
curl -X POST -H "Content-Type: application/json" --data '{"name":"elastic-sink","config" : { "connector.class":"com.datamountaineer.streamreactor.connect.elastic.ElasticSinkConnector","tasks.max":"1", "topics":"test_table","url":"localhost:9300", "cluster.name":"elasticsearch"}}' http://localhost:8083/connectors
```

## Improvements

