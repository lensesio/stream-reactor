![](../images/DM-logo.jpg)
[![Documentation Status](https://readthedocs.org/projects/streamreactor/badge/?version=latest)](http://streamreactor.readthedocs.io/en/latest/kudu.html#kafka-connect-kudu)

# Kafka Connect Kudu

A Connector and Sink to write events from Kafka to Kudu. 

Assumes SinkRecord schema matches Kudu table schema. Kudu tables must be precreated.

Writing with the same primary key twice will result in a Kudu write failure. See [this](http://getkudu.io/docs/kudu_impala_integration.html#impala_insertion_caveat)

## Perquisites
* Confluent 2.0
* Kudu 0.6
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

## Perquisites

* Cassandra 2.2.4
* Kudu Quickstart

## Properties

In addition to the default topics configuration the following options are added:

name | data type | required | description
-----|-----------|----------|------------
connect.kudu.master | string | yes | Kudu master host and port


Example connector.properties file

```bash 
name=kudu-sink
connector.class=com.datamountaineer.streamreactor.connect.kudu.KuduSinkConnector
tasks.max=1
connect.kudu.master=quickstart
topics=kudu_test
```


## Setup

* [Download Kudu Virtual Box](http://getkudu.io/docs/quickstart.html)

```bash
curl -s https://raw.githubusercontent.com/cloudera/kudu-examples/master/demo-vm-setup/bootstrap.sh | bash
```

* [Download and install Confluent](http://www.confluent.io/)
* Copy the Kudu sink jar from your build location to `$CONFLUENT_HOME/share/java/kafka-connect-kudu`

```bash
mkdir $CONFLUENT_HOME/share/java/kafka-connect-kudu
cp target/kafka-connect-kudu-0.1-jar-with-dependencies.jar  $CONFLUENT_HOME/share/java/kafka-connect-kudu/
```

* Start Confluents Zookeeper, Kafka and Schema registry

```bash
nohup $CONFLUENT_HOME/bin/zookeeper-server-start $CONFLUENT_HOME/etc/kafka/zookeeper.properties &
nohup $CONFLUENT_HOME/bin/kafka-server-start $CONFLUENT_HOME/etc/kafka/server.properties &
nohup $CONFLUENT_HOME/bin/schema-registry-start $CONFLUENT_HOME/etc/schema-registry/schema-registry.properties &"
```

* Create Kudu table

Log on to Kudu quickstart demo and create table

```bash
ssh demo@quickstart -t impala-shell

CREATE TABLE default.kudu_test (id INT,random_field STRING  ) TBLPROPERTIES ('kudu.master_addresses'='127.0.0.1', 'kudu.key_columns'='id', 'kudu.table_name'='kudu_test', 'transient_lastDdlTime'='1456744118', 'storage_handler'='com.cloudera.kudu.hive.KuduStorageHandler') 
exit;
```

* Start Kudu sink
 
```bash
export CLASSPATH=$CONFLUENT_HOME/share/java/kafka-connect-kudu/kafka-connect-kudu-0.1-jar-with-dependencies.jar

$CONFLUENT_HOME/bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties etc/kafka-connect-kudu/kudu.properties
```  


* Test with avro console, start the console to create the topic and write values

```bash
$CONFLUENT_HOME/bin/kafka-avro-console-producer \
--broker-list localhost:9092 --topic kudu_test \
--property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"id","type":"int"}, {"name":"random_field", "type": "string"}]}'
```
    
```bash
#insert at prompt
{"id": 999, "random_field": "foo"}
{"id": 888, "random_field": "bar"}
```
    
* Check in Impala

Check the logs of Kafka connect for the write and check in Impala (quickstart vm)

```bash 
ssh demo@quickstart -t impala-shell

SELECT * FROM kudu_test;
```

## Distributed Deployment
    
Kafka Connect is intended to be run as a service. A number of nodes and join together to form a 'leaderless' cluster. Each node or worker in
the cluster is also running a REST API to allow submitting, stopping and viewing running tasks.

To start in distributed mode run the following (note we only pass in one properties file):

```bash
export CLASSPATH=kafka-connect-kudu-0.1-jar-with-dependencies.jar
$CONFLUENT_HOME/bin/connect-distributed etc/schema-registry/connect-avro-distributed.properties
```

Now you can post in your task configuration

```bash
curl -X POST -H "Content-Type: application/json" --data '{"name":"kudu-sink", "config" : {"connector.class":"com.datamountaineer.streamreactor.connect.kudu.KuduSinkConnector","tasks.max":"1","kudu.master";"127.0.0.1","topics":"kafka_test"}}' http://localhost:8083/connectors
```

## Improvements

* Limited unit tests
* Schema evolution, not sure yet about's Kudu's abilities here
* No logging of failed writes.
* Auto create Kudu tables based on SinkRecord's schema? What to use as primary keys. What about data distribution and encoding?
* Add upsert when Kudu supports
