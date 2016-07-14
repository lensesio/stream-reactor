![](../images/DM-logo.jpg)
[![Documentation Status](https://readthedocs.org/projects/streamreactor/badge/?version=latest)](http://streamreactor.readthedocs.io/en/latest/hbase.html#kafka-connect-hbase)

## Kafka Connect JMS Sink

A Kafka Sink Connector to write the records to JMS queue or topic. The connector takes the value from the Kafka Connect SinkRecord and
puts it on the JMS topic/queue.

## Prerequisites

* Any JMS framework (i.e. ActiveMQ)
* Confluent 2.0
* Java 1.8 
* Scala 2.11

## Properties

In addition to the default topics configuration the following options are added:

| name       | data type           | required|  description|
|-----|-----------|----------|------------|
| connect.jms.sink.url | String | Yes | Specifies the JMS endpoint to connect to |
| connect.jms.sink.user | String | | Specifies the user to connect to the JMS endpoint. |
| connect.jms.sink.password | String | | Specifies the password for the user to connect to the JMS endpoint. |
| connect.jms.sink.connection.factory | String | Yes| Specifies the JMS connection factory class |
| connect.jms.sink.export.route.query | String | Yes| The KCQL expressing what gets sourced from Kafka and where it lands in JMS. Check: https://github.com/datamountaineer/kafka-connect-query-language for details|
| connect.jms.sink.export.route.topics | String | Yes| Specifies the JMS topics. The KCQL target points to the JMS destination. If the destination is meant to a topic then it should be present in this list|
| connect.jms.sink.export.route.queues| String | Yes| Specifies the JMS queues. The KCQL target points to the JMS destination. If the destination is meant to a queue then it should be present in this list|
| connect.jms.sink.message.type| String | Yes| Specifies the JMS payload. If JSON is chosen it will send a TextMessage; if AVRO is chosen it will send a BytesMessage;if MAP is chosen it will send a MapMessage;if OBJECT is chosen it will send an ObjectMessage|
| connect.jms.error.policy| String | Yes| There are two available options: NOOP - the error is swallowed ;THROW - the error is allowed to propagate. RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on the error will be logged automatically|
| connect.jms.retry.interval | INT | | The time elapsed between retrying to send the messages to the JMS in case of a previous error|

Example jms.properties file

```bash

connector.class=com.datamountaineer.streamreactor.connect.jms.sink.JMSSinkConnector
tasks.max=1
topics=person_jms
name=person-jms-test

connect.jms.sink.url=tcp://somehost:61616
connect.jms.sink.connection.factory=org.apache.activemq.ActiveMQConnectionFactory
connect.jms.sink.export.route.query=INSERT INTO topic_1 SELECT * FROM person_jms
connect.jms.sink.message.type=AVRO
connect.jms.sink.export.route.topics=person_jms
connect.jms.sink.export.route.queues=
connect.jms.error.policy=THROW
```

## Setup

* Clone and build the Connector and Sink

```bash
git clone git@github.com:andrewstevenson/stream-reactor.git
cd stream-reactor/kafka-connect
gradle fatJar
```

* [Download and install Confluent](http://www.confluent.io/)
* Copy the JMS sink jar from your build location to `$CONFLUENT_HOME/share/java/kafka-connect-jms`

```bash
mkdir ${CONFLUENT_HOME}/share/java/kafka-connect-jms
cp target/kafka-connect-jms-0.1-all.jar  $CONFLUENT_HOME/share/java/kafka-connect-jms/
```

* Start  Kafka and Schema registry
By running HBase the zookeeper instance is already present so there is no need to start the one coming with confluent

```bash
nohup $CONFLUENT_HOME/bin/kafka-server-start $CONFLUENT_HOME/etc/kafka/server.properties > /dev/null 2>&1 &
nohup $CONFLUENT_HOME/bin/schema-registry-start $CONFLUENT_HOME/etc/schema-registry/schema-registry.properties > /dev/null 2>&1 &
```
    

* Start Kafka Connect with the JMS sink


```bash
$CONFLUENT_HOME/bin/connect-standalone ../etc/schema-registry/connect-avro-standalone.properties ../etc/kafka-connect-jms/jms.properties
```

* Test with avro console, start the console to create the topic and write values

```bash
$CONFLUENT_HOME/bin/kafka-avro-console-producer \
  --broker-list localhost:9092 --topic person_jms\
  --property value.schema='{"type":"record","name":"User","namespace":"com.datamountaineer.streamreactor.connect.jms","fields":[{"name":"firstName","type":"string"},{"name":"lastName","type":"string"},{"name":"age","type":"int"},{"name":"salary","type":"double"}]}'
```

```bash
{"firstName": "John", "lastName": "Smith", "age":30, "salary": 4830}
{"firstName": "Anna", "lastName": "Jones", "age":28, "salary": 5430}
```

## Distributed Deployment
    
Kafka Connect is intended to be run as a service. A number of nodes and join together to form a 'leaderless' cluster. Each node or worker in
the cluster is also running a REST API to allow submitting, stopping and viewing running tasks.

To start in distributed mode run the following (note we only pass in one properties file):

```bash
export CLASSPATH=kafka-connect-jms-0.1-all.jar
$CONFLUENT_HOME/bin/connect-distributed etc/schema-registry/connect-avro-distributed.properties
```

Now you can post in your task configuration

```bash
curl -X POST -H "Content-Type: application/json" --data '{"name":"jms-sink","config": {$JSON_OF_YOUR_SINK_CONFIG}}' http://localhost:8083/connectors
```
