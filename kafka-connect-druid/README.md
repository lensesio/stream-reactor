![](../images/DM-logo.jpg)
[![Documentation Status](https://readthedocs.org/projects/streamreactor/badge/?version=latest)](http://streamreactor.readthedocs.io/en/latest/druid.html#kafka-connect-druid)


# Kafka Connect Druid

PLACEHOLDER --NOT COMPLETE---- VOLUNTEERS!

WORK IN PROGRESS

## Prerequisites

* Confluent 3.0.0
* Druid Tranquility 0.7.4
* Java 1.8 
* Scala 2.11

## Properties

In addition to the default topics configuration the following options are added:

name | data type | required | description
-----|-----------|----------|------------


Example connector.properties file

```bash 
name=cassandra-sink
connector.class=com.datamountaineer.streamreactor.connect.druid.DruidSinkConnector
tasks.max=1
topics=test_table
```


## Setup

* Clone and build the Connector and Sink

```bash
git clone git@github.com:andrewstevenson/stream-reactor.git
cd stream-reactor/kafka-connect
mvn package
```

* [Download and install Cassandra](http://cassandra.apache.org/)
* [Download and install Confluent](http://www.confluent.io/)
* Copy the Druid sink jar from your build location to `$CONFLUENT_HOME/share/java/kafka-connect-druid`

```bash
mkdir $CONFLUENT_HOME/share/java/kafka-connect-duid
cp target/kafka-connect-druid-0.1-jar-with-dependencies.jar  $CONFLUENT_HOME/share/java/kafka-connect-druid/
```
    

    
* Start Confluents Zookeeper, Kafka and Schema registry

```bash
nohup $CONFLUENT_HOME/bin/zookeeper-server-start $CONFLUENT_HOME/etc/kafka/zookeeper.properties &
nohup $CONFLUENT_HOME/bin/kafka-server-start $CONFLUENT_HOME/etc/kafka/server.properties &
nohup $CONFLUENT_HOME/bin/schema-registry-start $CONFLUENT_HOME/etc/schema-registry/schema-registry.properties &"
```
    


* If you have the Elastic Sink Connector
Elastic Search is on Netty 3.10 so to avoid conflicts we need our Elastic Sink Connector first in the classpath

```bash
export CLASSPATH=$CONFLUENT_HOME/share/java/kafka-connect-elastic/kafka-connect-elastic-0.1-jar-with-dependencies.jar;export CLASSPATH=$CONFLUENT_HOME/share/java/kafka-connect-cassandra/kafka-connect-cassandra-0.1-jar-with-dependencies.jar

$CONFLUENT_HOME/bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties etc/kafka-connect-cassandra/cassandra.properties
```

* Test with avro console, start the console to create the topic and write values

```bash
$CONFLUENT_HOME/bin/kafka-avro-console-producer \
  --broker-list localhost:9092 --topic test_table \
  --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"id","type":"int"}, {"name":"random_field", "type": "string"}]}'
```

```bash
{"id": 999, "random_field": "foo"}
{"id": 888, "random_field": "bar"}
```
    


## Distributed Deployment
    
Kafka Connect is intended to be run as a service. A number of nodes and join together to form a 'leaderless' cluster. Each node or worker in
the cluster is also running a REST API to allow submitting, stopping and viewing running tasks.

To start in distributed mode run the following (note we only pass in one properties file):

```bash
export CLASSPATH=kafka-connect-druid-0.1-jar-with-dependencies.jar
$CONFLUENT_HOME/bin/connect-distributed etc/schema-registry/connect-avro-distributed.properties
```


Now you can post in your task configuration

## Improvements
