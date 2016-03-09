# Kafka Connect Bloomberg

A Connector to Subscribe to Bloomberg ticker updates and write the data to Kafka. The connector will create and open a
Bloomberg session and will subscribe for data updates given the configuration provided. As data is received from Bloomberg
it will be added to a blocking queue. When the kafka connect framework polls the source task for data it will drain this
queue.

When the session is created it will be linked to a Bloomberg service. There are only two supported: "//blp/mkdata" and
"//blp/refdata".

The configuration can specify different tickers to subscribe for and for each one it can specify a different fields. See in the Properties section more details

## Perquisites
* Confluent 2.0

## Properties

In addition to the default topics configuration the following options are added:

name | data type | required | description
-----|-----------|----------|------------
server.host|string|yes|The Bloomberg endpoint host to connect to
server.port|int|yes| The Bloomberg endpoint port number to connect to
service.uri|string|yes| Which Bloomberg service to connect to. Can be //blp/mkdata or //blp/refdata
authentication.mode|string|no| There are two modes supported by the Bloomberg API:APPLICATION_ONLY or USER_AND_APPLICATION; Check the Bloomberg documentation for details
bloomberg.subscriptions|string|yes| Specifies which ticker subscription to make. The format is TICKER:FIELD,FIELD,..;TICKER:FIELD,FIELD,... etc. And example is: AAPL US Equity:LAST_PRICE,BID,ASK;IBM US Equity:BID,ASK,HIGH,LOW,OPEN
kafka.topic|string|yes|The Kafka topic to send the data to
buffer.size|int|no| The buffer accumulating the data updates received from Bloomberg. If not provided it will default to 2048. If the buffer is full and a new update will be received it won't be added to the buffer until it is first drained
payload.type|string|no| Specifies the payload type going over kafka. There are two supported mode:json(default) and avro.

Example connector.properties file

```bash 
name=bloomberg-source
connector.class=com.datamountaineer.streamreactor.connect.bloomberg.BloombergSourceConnector
tasks.max=1
server.host=localhost
server.port=8194
service.uri=//blp/mkdata
bloomberg.subscriptions=AAPL US Equity:LAST_PRICE,BID,ASK;IBM US Equity:BID,ASK,HIGH,LOW,OPEN
kafka.topic=bloomberg
buffer.size=4096
```

## Payload
The content of the message passed to Kafka is as follows
{
  "subscription": "$subscription_key/ticker"
  "fields":{
    "field1" : "value1"
    "field2" : "value2"
    ...
  }
}

All the fields are driven by the subscription defined in the config for the subscription_key/ticker

## Setup
* Clone and build the Connector and Sink

```bash
git clone git@github.com:andrewstevenson/stream-reactor.git
cd stream-reactor/kafka-connect-bloomberg
mvn package
```

* [Download and install Confluent](http://www.confluent.io/)
* Copy the Bloomberg source jar from your build location to `$CONFLUENT_HOME/share/java/kafka-connect-bloomberg`

```bash
mkdir $CONFLUENT_HOME/share/java/kafka-connect-bloomberg
cp target/kafka-connect-bloomberg-0.1-jar-with-dependencies.jar $CONFLUENT_HOME/share/java/kafka-connect-bloomberg/
```

* Start Confluents Zookeeper, Kafka and Schema registry

```bash
nohup $CONFLUENT_HOME/bin/zookeeper-server-start $CONFLUENT_HOME/etc/kafka/zookeeper.properties &
nohup $CONFLUENT_HOME/bin/kafka-server-start $CONFLUENT_HOME/etc/kafka/server.properties &
nohup $CONFLUENT_HOME/bin/schema-registry-start $CONFLUENT_HOME/etc/schema-registry/schema-registry.properties &"
```


* Start Kafka Connect in standalone with the Bloomberg source

```bash
$CONFLUENT_HOME/bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties etc/kafka-connect-bloomberg/bloomberg.properties
```

* Test with avro console, start the console to create the topic and read the values

```bash
$CONFLUENT_HOME/bin/kafka-avro-console-consumer \
 --zookeeper localhost:2181 --topic bloomberg \
 --from-beginning
```

## Distributed Deployment
    
Kafka Connect is intended to be run as a service. A number of nodes and join together to form a 'leaderless' cluster. Each node or worker in
the cluster is also running a REST API to allow submitting, stopping and viewing running tasks.

To start in distributed mode run the following (note we only pass in one properties file):

```bash
export CLASSPATH=kafka-connect-bloomberg-0.1-jar-with-dependencies.jar
$CONFLUENT_HOME/bin/connect-distributed etc/schema-registry/connect-avro-distributed.properties
```

Now you can post in your task configuration

## Improvements
  - Upgrade the bloomberg emulator api to work with the latest version of the bloomberg api
  - Push in Avro instead of a json string - Avro4s or Kite SDK to convert to Avro or convert the whole of Bloomberg Constatnts to a massive schema
