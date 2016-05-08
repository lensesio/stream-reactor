![](../images/DM-logo.jpg)

## Kafka Connect Hbase

A Connector and Sink to write events from Kafka to Hbase. The connector takes the value from the Kafka Connect SinkRecords and inserts a new row into Hbase.

When inserting a row in Hbase you need a row key. There are 3 different settings:
*SINK_RECORD - it grabs the SinkRecord.keyValue
*GENERIC - It combines the SinkRecord kafka topic, kafka offset and kafka partition to form the key
*FIELDS - It combines all the specified payload fields to form the row key.

The task expects the table in HBase to exist. By default all the fields in the record would be added to the hbase table and column familty. However you can specify the fields to insert and the column to insert to (see the configuration table below).

## Perquisites

* Hbase 1.2.0
* Confluent 2.0
* Java 1.8 
* Scala 2.11

## Properties

In addition to the default topics configuration the following options are added:

| name       | data type           | required|  description|
|-----|-----------|----------|------------|
| connect.hbase.sink.table.rowkey.mode | String | Yes | There are three available modes: SINK_RECORD, FIELDS and GENERIC. SINK_RECORD - uses the SinkRecord.keyValue as the Hbase row key; FIELDS - combines the specified payload (kafka connect Struct instance) fields to make up the Hbase row key; GENERIC- combines the kafka topic, offset and partition to build the Hbase row key. |
| connect.hbase.sink.key | String | | If row key mode is set to FIELDS this setting is required. Multiple fields can be specified by separating them via a comma; The fields are combined using a key separator by default is set to <\\n>. |
| connect.hbase.sink.table.name | String | Yes | Specifies the target Hbase table to insert into |
|connect.hbase.sink.column.family | String | Yes | Specifies the table column family to use when inserting the new entry columns |
|connect.hbase.sink.fields | String | | Specifies which fields to consider when inserting the new Hbase entry. If is not set it will take all the fields present in the payload. Field mapping is supported; this way a payload field can be inserted into a 'mapped' column. If this setting is not present it will insert all fields.  Examples: * fields to be used:field1,field2,field3; - Only! field1,field2 and field3 will be inserted ** fields with mapping: field1=alias1,field2,field3=alias3 - Only! field1, field2 and field3 will be inserted *** fields with mapping:*,field3=alias - All fields are inserted but field3 will be inserted as 'alias' |

Example hbase.properties file

```bash
connect.hbase.sink.rowkey.mode=FIELDS
connect.hbase.sink.table.name=person
connect.hbase.sink.column.family=d
connect.hbase.sink.key=firstName,lastName
connect.hbase.sink.fields=firstName,lastName,age,salary=income

connector.class=com.datamountaineer.streamreactor.connect.hbase.HbaseSinkConnector
tasks.max=1
topics=person_hbase
name=person-hbase-test
```

## Setup

* Clone and build the Connector and Sink

```bash
git clone git@github.com:andrewstevenson/stream-reactor.git
cd stream-reactor/kafka-connect
gradle fatJar
```

* [Download and install Hbase](http://hbase.apache.org/0.94/book/quickstart.html)
* [Download and install Confluent](http://www.confluent.io/)
* Copy the Hbase sink jar from your build location to `$CONFLUENT_HOME/share/java/kafka-connect-hbase`

```bash
mkdir $CONFLUENT_HOME/share/java/kafka-connect-hbase
cp target/kafka-connect-hbase-0.1-all.jar  $CONFLUENT_HOME/share/java/kafka-connect-hbase/
```
    
* Start Hbase

```bash
./$HBASE_HOME/bin/start-hbase.sh
```
    
* Start  Kafka and Schema registry
By running HBase the zookeeper instance is already present so there is no need to start the one coming with confluent

```bash
nohup $CONFLUENT_HOME/bin/kafka-server-start $CONFLUENT_HOME/etc/kafka/server.properties > /dev/null 2>&1 &
nohup $CONFLUENT_HOME/bin/schema-registry-start $CONFLUENT_HOME/etc/schema-registry/schema-registry.properties > /dev/null 2>&1 &
```
    
* Create Hbase table

```sql
create 'person',{NAME=>'d', VERSIONS=>1}
```

    
* Start Kafka Connect with the Hbase sink


```bash
$CONFLUENT_HOME/bin/connect-standalone ../etc/schema-registry/connect-avro-standalone.properties ../etc/kafka-connect-hbase/hbase.properties
```

* Test with avro console, start the console to create the topic and write values

```bash
$CONFLUENT_HOME/bin/kafka-avro-console-producer \
  --broker-list localhost:9092 --topic person_hbase \
  --property value.schema='{"type":"record","name":"User","namespace":"com.datamountaineer.streamreactor.connect.hbase","fields":[{"name":"firstName","type":"string"},{"name":"lastName","type":"string"},{"name":"age","type":"int"},{"name":"salary","type":"double"}]}'
```

```bash
{"firstName": "John", "lastName": "Smith", "age":30, "salary": 4830}
{"firstName": "Anna", "lastName": "Jones", "age":28, "salary": 5430}
```
    
* Check in Hbase for the records

```sql
get 'person', 'd'
``` 

## Distributed Deployment
    
Kafka Connect is intended to be run as a service. A number of nodes and join together to form a 'leaderless' cluster. Each node or worker in
the cluster is also running a REST API to allow submitting, stopping and viewing running tasks.

To start in distributed mode run the following (note we only pass in one properties file):

```bash
export CLASSPATH=kafka-connect-hbase-0.1-all.jar
$CONFLUENT_HOME/bin/connect-distributed etc/schema-registry/connect-avro-distributed.properties
```

Now you can post in your task configuration

```bash
curl -X POST -H "Content-Type: application/json" --data '{"name":"hbase-sink","config": {"connect.hbase.sink.rowkey.mode":"FIELDS", "connect.hbase.sink.table.name":"person","connect.hbase.sink.column.family":"d",  "connect.hbase.sink.key":"firstName,lastName","connect.hbase.sink.fields":"firstName,lastName,age,salary=income","connector.class":"com.datamountaineer.streamreactor.connect.hbase.HbaseSinkConnector","tasks.max":"1","topics":"person_hbase"}}' http://localhost:8083/connectors
```

## Improvements
* Mapping  record fields in the incoming payload to different tables
* Mapping  nested objects from the incoming payload to different tables
