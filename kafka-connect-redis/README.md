## Kafka Connect Redis

A Connector and Sink to write events from Kafka to Redis. The connector takes the value from the Kafka Connect SinkRecords and inserts a new entry to Redis.

When inserting a row in redis you need a row key. There are 3 different settings:
*SINK_RECORD - it grabs the SinkRecord.keyValue
*GENERIC - It combines the SinkRecord kafka topic, kafka offset and kafka partition to form the key
*FIELDS - It combines all the specified payload fields to form the row key.

The task expects the table in Redis to exist. By default all the fields in the record would be added to the redis table and column familty. However you can specify the fields to insert and the column to insert to (see the configuration table below).

## Perquisites

* Jedis 2.8.1
* Confluent 2.0
* Java 1.8 
* Scala 2.11

## Properties

In addition to the default topics configuration the following options are added:

| name       | data type           | required|  description|
|-----|-----------|----------|------------|
| connect.redis.sink.connection.host| String | Yes | Specifies the Redis server |
| connect.redis.sink.connection.port| String | Yes | Specifies the Redis server port number |
| connect.redis.sink.connection.password| String |  | Specifies the authorization password |
| connect.redis.sink.key.mode | String | Yes | There are three available modes: SINK_RECORD, FIELDS and GENERIC. SINK_RECORD - uses the SinkRecord.keyValue as the redis row key; FIELDS - combines the specified payload (kafka connect Struct instance) fields to make up the redis row key; GENERIC- combines the kafka topic, offset and partition to build the redis row key. |
| connect.redis.sink.keys | String | | If row key mode is set to FIELDS this setting is required. Multiple fields can be specified by separating them via a comma; The fields are combined using a key separator by default is set to <\\n>. |
|connect.redis.sink.fields | String | | Specifies which fields to consider when inserting the new Redis entry. If is not set it will take all the fields present in the payload. Field mapping is supported; this way a payload field can be inserted into a 'mapped' column. If this setting is not present it will insert all fields.  Examples: * fields to be used:field1,field2,field3; - Only! field1,field2 and field3 will be inserted ** fields with mapping: field1=alias1,field2,field3=alias3 - Only! field1, field2 and field3 will be inserted *** fields with mapping:*,field3=alias - All fields are inserted but field3 will be inserted as 'alias' |

Example redis.properties file

```bash
name=redis
connect.redis.sink.key.mode=FIELDS
connect.redis.sink.keys=firstName,lastName
connect.redis.sink.fields=firstName,lastName,age,salary=income

connect.redis.connection.host=localhost
connect.redis.connection.port=6379

connector.class=com.datamountaineer.streamreactor.connect.redis.sink.RedisSinkConnector
tasks.max=1
topics=person_redis
```

## Setup

* Clone and build the Connector and Sink

```bash
git clone git@github.com:andrewstevenson/stream-reactor.git
cd stream-reactor/kafka-connect
gradle fatJar
```

* [Download and instal_Redis](http://redis.io)
* [Download and install Confluent](http://www.confluent.io/)
* Copy the Redis sink jar from your build location to `$CONFLUENT_HOME/share/java/kafka-connect-redis`

```bash
mkdir $CONFLUENT_HOME/share/java/kafka-connect-redis
cp target/kafka-connect-redis-0.1-all.jar  $CONFLUENT_HOME/share/java/kafka-connect-redis/
```
    
* Make sure your redis services is running

```bash
sudo service redis-server status redis-server is running
```
    
* Start  Zookeepr, Kafka and Schema registry

```bash
nohup $CONFLUENT_HOME/bin/zookeeper-server-start $CONFLUENT_HOME/etc/kafka/zookeeper.properties > /dev/null 2>&1 &
nohup $CONFLUENT_HOME/bin/kafka-server-start $CONFLUENT_HOME/etc/kafka/server.properties > /dev/null 2>&1 &
nohup $CONFLUENT_HOME/bin/schema-registry-start $CONFLUENT_HOME/etc/schema-registry/schema-registry.properties > /dev/null 2>&1 &
```

    
* Start Kafka Connect with the Redis sink


```bash
$CONFLUENT_HOME/bin/connect-standalone ../etc/schema-registry/connect-avro-standalone.properties ../etc/kafka-connect-redis/redis.properties
```

* Test with avro console, start the console to create the topic and write values

```bash
$CONFLUENT_HOME/bin/kafka-avro-console-producer \
  --broker-list localhost:9092 --topic person_redis \
  --property value.schema='{"type":"record","name":"User","namespace":"com.datamountaineer.streamreactor.connect.redis","fields":[{"name":"firstName","type":"string"},{"name":"lastName","type":"string"},{"name":"age","type":"int"},{"name":"salary","type":"double"}]}'
```

```bash
{"firstName": "John", "lastName": "Smith", "age":30, "salary": 4830}
{"firstName": "Anna", "lastName": "Jones", "age":28, "salary": 5430}
```
    
* Check in Redis for the records

```bash
redis-cli
> get (lists all keys)
> GET "John.Smith" (get this specific key)
 "{\"firstName\":\"John\",\"lastName\":\"Smith\",\"age\":30,\"income\":4830.0}"

``` 

## Distributed Deployment
    
Kafka Connect is intended to be run as a service. A number of nodes and join together to form a 'leaderless' cluster. Each node or worker in
the cluster is also running a REST API to allow submitting, stopping and viewing running tasks.

To start in distributed mode run the following (note we only pass in one properties file):

```bash
export CLASSPATH=kafka-connect-redis-0.1-all.jar
$CONFLUENT_HOME/bin/connect-distributed etc/schema-registry/connect-avro-distributed.properties
```

Now you can post in your task configuration

```bash
curl -X POST -H "Content-Type: application/json" --data '{"name":"reid-sink","config": {"connect.redis.sink.key.mode":"FIELDS","connect.redis.sink.keys":"firstName,lastName","connect.redis.sink.fields":" firstName,lastName,age,salary":"income","connect.redis.connection.host":"$SERVER","connect.redis.connection.port":" 6379","connector.class":" com.datamountaineer.streamreactor.connect.redis.sink.RedisSinkConnector","tasks.max":"1","topics":"person_redis"}}' http://localhost:8083/connectors
```

## Improvements
* Allow the entire payload to be written as json (currently if we have nested structs won't be considered)
* Write avro in the Redis value?!
