# Kafka Connect Cassandra

A Connector and Sink to write events from Kafka to Cassandra. The connector converts the value from the Kafka Connect SinkRecords to Json and uses Cassandra's JSON insert functionality to insert the rows.

PreparedStatements are created and cached at startup for the topics assigned to the task.

The task expects a table in Cassandra per topic that the sink is configured to listen to. You could have different schemas in the same topic, Cassandra's JSON functionality will ignore missing columns.

The table and keyspace must be created before hand! The sink tries to write to a table matching the name of the topic. The prepared statements will blow if the corresponding table does not exist.

## Perquisites

* Cassandra 2.2.4
* Confluent 2.0

## Properties

In addition to the default topics configuration the following options are added:

name | data type | required | description
-----|-----------|----------|------------
contact.points | string | yes | contact points (hosts) in Cassandra cluster
key.space | string | yes | key_space the tables to write to belong to
port | int | no | port for the native Java driver (default 9042)

Example connector.properties file

```bash 
name=cassandra-sink
connector.class=com.datamountaineer.streamreactor.connect.cassandra.CassandraSinkConnector
tasks.max=1
topics=test_table
contact.points=localhost
port=9042
key.space=connect_test
```

You must also supply the `connector.class` as `com.datamountaineer.streamreactor.connect.cassandra.CassandraSinkConnector`

## Setup

* Clone and build the Connector and Sink

```bash
git clone git@github.com:andrewstevenson/stream-reactor.git
cd stream-reactor/kafka-connect
mvn package
```

* [Download and install Cassandra](http://cassandra.apache.org/)
* [Download and install Confluent](http://www.confluent.io/)
* Copy the Cassandra sink jar from your build location to `$CONFLUENT_HOME/share/java/kafka-connect-cassandra`

```bash
mkdir $CONFLUENT_HOME/share/java/kafka-connect-cassandra
cp target/kafka-connect-cassandra-0.1-jar-with-dependencies.jar  $CONFLUENT_HOME/share/java/kafka-connect-cassandra/
```
    
* Start Cassandra

```bash
nohup $CASSANDRA_HOME/bin/cassandra &
```
    
* Start Confluents Zookeeper, Kafka and Schema registry

```bash
nohup $CONFLUENT_HOME/bin/zookeeper-server-start $CONFLUENT_HOME/etc/kafka/zookeeper.properties &
nohup $CONFLUENT_HOME/bin/kafka-server-start $CONFLUENT_HOME/etc/kafka/server.properties &
nohup $CONFLUENT_HOME/bin/schema-registry-start $CONFLUENT_HOME/etc/schema-registry/schema-registry.properties &"
```
    
* Create keyspace in Cassandra

```sql
$CASSANDRA_HOME/bin/cqlsh
```

```sql
CREATE KEYSPACE connect_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}  
AND durable_writes = true;

CREATE TABLE connect_test.test_table (
id int PRIMARY KEY,
random_field text
); 
```
    
* Start Kafka Connect with the Cassandra sink

* If you have the Elastic Sink Connector
Elastic Search is on Netty 3.10 so to avoid conflicts we need our Elastic Sink Connector first in the classpath

```bash
export CLASSPATH=$CONFLUENT_HOME/share/java/kafka-connect-elastic/kafka-connect-elastic-0.1-jar-with-dependencies.jar;

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
    
* Check in Cassandra for the records

```sql
SELECT * FROM connect_test.test_table
``` 

## Distributed Deployment
    
Kafka Connect is intended to be run as a service. A number of nodes and join together to form a 'leaderless' cluster. Each node or worker in
the cluster is also running a REST API to allow submitting, stopping and viewing running tasks.

To start in distributed mode run the following (note we only pass in one properties file):

```bash
export CLASSPATH=kafka-connect-cassandra-0.1-jar-with-dependencies.jar
$CONFLUENT_HOME/bin/connect-distributed etc/schema-registry/connect-avro-distributed.properties
```

Now you can post in your task configuration

```bash
curl -X POST -H "Content-Type: application/json" --data '{"name":"cassandra-sink","config": {"connector.class":"com.datamountaineer.streamreactor.connect.cassandra.CassandraSinkConnector","tasks.max":"1","topics":"test_table","contact.points":"localhost","port":"9042","key.space":"connect_test"}}' http://localhost:8083/connectors
```

## Improvements
* Add key of message to payload
* Auto create tables in Cassandra if they don't exists. Need a converter from Connect data types to Cassandra
