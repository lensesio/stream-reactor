![](../images/DM-logo.jpg)
[![Documentation Status](https://readthedocs.org/projects/streamreactor/badge/?version=latest)](http://streamreactor.readthedocs.io/en/latest/cassandra.html#kafka-connect-cassandra)

# Kafka Connect Cassandra

Kafka Connect Cassandra is a Source Connector for reading data from Cassandra and a Sink Connector for writing data to Cassandra.

## Prerequisites

* Cassandra 2.2.4
* Confluent 2.0
* Java 1.8 
* Scala 2.11

## Setup

Before we can do anything, including the QuickStart we need to install Cassandra and the Confluent platform.

### Cassandra Setup

First download and install Cassandra if you don't have a compatible cluster available.

```bash
#make a folder for cassandra
mkdir cassandra

#Download Cassandra
wget http://apache.cs.uu.nl/cassandra/3.5/apache-cassandra-3.5-bin.tar.gz

#extract archive to cassandra folder
tar -xvf apache-cassandra-3.5-bin.tar.gz -C cassandra

#Set up environment variables
export CASSANDRA_HOME=~/cassandra/apache-cassandra-3.5-bin
export PATH=$PATH:$CASSANDRA_HOME/bin

#Start Cassandra
sudo sh ~/cassandra/bin/cassandra
```

### Confluent Setup

```bash
#make confluent home folder
mkdir confluent

#download confluent
wget http://packages.confluent.io/archive/2.0/confluent-2.0.1-2.11.7.tar.gz

#extract archive to confluent folder
tar -xvf confluent-2.0.1-2.11.7.tar.gz -C confluent

#setup variables
export CONFLUENT_HOME=~/confluent/confluent-2.0.1
```

Enable topic deletion.

In */etc/kafka/server.properties* add the following to we can delete topics.

```
delete.topic.enable=true
```

Start the Confluent platform.

```bash
#Start the confluent platform, we need kafka, zookeeper and the schema registry
bin/zookeeper-server-start etc/kafka/zookeeper.properties &
bin/kafka-server-start etc/kafka/server.properties &
bin/schema-registry-start etc/schema-registry/schema-registry.properties &
```

#### Build the Connector and CLI

The prebuilt jars can be taken from here and [here](https://github.com/datamountaineer/kafka-connect-tools/releases) or from Maven. 

[<img src="https://img.shields.io/badge/latest%20release-v0.2-blue.svg?label=Kafka Connect CLI latest%20release"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22kafka-connect-cli%22)

If you want to build the connector, clone the repo and build the jar.

```bash
##Build the connectors
git clone https://github.com/datamountaineer/stream-reactor
cd stream-reactor
gradle fatJar

##Build the CLI for interacting with Kafka connectors
git clone https://github.com/datamountaineer/kafka-connect-tools
cd kafka-connect-tools
gradle fatJar
```

## Source Connector

The Cassandra source connector allows you to extract entries from Cassandra with the CQL driver and write them into a Kafka topic.

Each table specified in the configuration is polled periodically and each record from the result is converted to a Kafka Connect record. These records are then written to Kafka by the Kafka Connect framework.

The source connector operates in two modes:

1. Bulk - Each table is selected in full each time it is polled.
2. Incremental - Each table is querying with lower and upper bounds to extract deltas.

In incremental mode the column used to identify new or delta rows has to be provided. This column must be of CQL Type Timestamp. Due to Cassandra's and CQL restrictions this should be a primary key or part of a composite primary keys. ALLOW_FILTERING can also be supplied as an configuration.

.note::TimeUUIDs are convert to strings. Use the [UUIDs](https://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/utils/UUIDs.html) helpers to convert to Dates.


### Source Connector QuickStart

To see the basic functionality of the Source connector we will start with the Bulk import mode.

#### Test data

Once you have installed and started Cassandra create a table to extract records from. This snippet creates a table called orders and inserts 3 rows representing fictional orders or some options and futures on a trading platform.

Start the Cassandra cql shell

```bash
➜  bin ./cqlsh
Connected to Test Cluster at 127.0.0.1:9042.
[cqlsh 5.0.1 | Cassandra 3.0.2 | CQL spec 3.3.1 | Native protocol v4]
Use HELP for help.
cqlsh> 
```

Execute the following:

```bash
CREATE KEYSPACE demo WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3};
use demo;

create table orders (id int, created timeuuid, product text, qty int, price float, PRIMARY KEY (id, created)) WITH CLUSTERING ORDER BY (created asc);

INSERT INTO orders (id, created, product, qty, price) VALUES (1, now(), 'OP-DAX-P-20150201-95.7', 100, 94.2);
INSERT INTO orders (id, created, product, qty, price) VALUES (2, now(), 'OP-DAX-C-20150201-100', 100, 99.5);
INSERT INTO orders (id, created, product, qty, price) VALUES (3, now(), 'FU-KOSPI-C-20150201-100', 200, 150);

SELECT * FROM orders;

 id | created                              | price | product                 | qty
----+--------------------------------------+-------+-------------------------+-----
  1 | 17fa1050-137e-11e6-ab60-c9fbe0223a8f |  94.2 |  OP-DAX-P-20150201-95.7 | 100
  2 | 17fb6fe0-137e-11e6-ab60-c9fbe0223a8f |  99.5 |   OP-DAX-C-20150201-100 | 100
  3 | 17fbbe00-137e-11e6-ab60-c9fbe0223a8f |   150 | FU-KOSPI-C-20150201-100 | 200

(3 rows)

(3 rows)
```

#### Source Connector Configuration (Bulk)

Next we start the connector in standalone mode. This useful for testing and one of jobs, usually you'd run in distributed mode to get fault tolerance and better performance.

Before we can start the connector we need to setup it's configuration. In standalone mode this is done by creating a properties file and passing this to the connector at startup. In distributed mode you can post in the configuration as json to the Connectors HTTP endpoint. Each connector exposes a rest endpoint for stoping, starting and updating the configuration.

Since we are in standalone mode we'll create a file called cassandra-source-standalone-orders.properties with the contents below:

```bash
name=cassandra-source-orders
connector.class=com.datamountaineer.streamreactor.connect.cassandra.source.CassandraSourceConnector
cassandra.key.space=demo
cassandra.import.map=orders:orders-topic
cassandra.import.mode=bulk
cassandra.authentication.mode=username_password
cassandra.contact.points=localhost
cassandra.username=cassandra
cassandra.password=cassandra
```
This configuration defines:

1. The name of the connector, must be unique.
2. The name of the connector class.
3. The keyspace (demo) we are connecting to.
4. The table to topic import map. This allows you to route tables to different topics. Each mapping is comma separated and for each mapping the table and topic are separated by a colon, if no topic is provide the records from the table will be routed to a topic matching the table name. In this example the orders table records are routed to the topic orders-topic. This property sets the tables to import!
5. The import mode, either incremental or bulk.
6. The authentication mode, this is either none or username_password. We haven't enabled this on our Cassandra install but you should.
7. The ip or host name of the nodes in the Cassandra cluster to connect to.
8. Username and password, ignored unless you have set Cassandra to use the PasswordAuthenticator. 

#### Starting the Source Connector (Standalone)

Now we are ready to start the Cassandra Source Connector in standalone mode.

..note:: You need to add the connector to your classpath or you can create a folder in share/java like kafka-connect-myconnector and the start scripts provided by Confluent will pick it up. The start script looks for folders beginning with kafka-connect.

```bash
#Add the Connector to the class path
➜  export CLASSPATH=kafka-connect-cassandra-0.1-all.jar
#Start the connector in standalone mode, passing in two properties files, the first for the schema registry, kafka and zookeeper and the second with the connector properties.
➜  bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties cassandra-source-standalone-orders.properties
```

We can use the CLI to check if the connector is up but you should be able to see this in logs as-well.

```bash
➜ java -jar build/libs/kafka-connect-cli-0.2-all.jar get cassandra-source-orders
#Connector `cassandra-source-orders`:
cassandra.key.space=demo
name=cassandra-source-orders
cassandra.import.mode=bulk
connector.class=com.datamountaineer.streamreactor.connect.cassandra.source.CassandraSourceConnector
cassandra.authentication.mode=username_password
cassandra.contact.points=localhost
cassandra.username=cassandra
cassandra.password=cassandra
cassandra.import.map=orders:orders-topic
#task ids: 0

```

#### Check for Source Records in Kafka

Now check the logs of the connector you should see this

```bash
   ____        __        __  ___                  __        _
   / __ \____ _/ /_____ _/  |/  /___  __  ______  / /_____ _(_)___  ___  ___  _____
  / / / / __ `/ __/ __ `/ /|_/ / __ \/ / / / __ \/ __/ __ `/ / __ \/ _ \/ _ \/ ___/
 / /_/ / /_/ / /_/ /_/ / /  / / /_/ / /_/ / / / / /_/ /_/ / / / / /  __/  __/ /
/_____/\__,_/\__/\__,_/_/  /_/\____/\__,_/_/ /_/\__/\__,_/_/_/ /_/\___/\___/_/
       ______                                __           _____
      / ____/___ _______________ _____  ____/ /________ _/ ___/____  __  _______________
     / /   / __ `/ ___/ ___/ __ `/ __ \/ __  / ___/ __ `/\__ \/ __ \/ / / / ___/ ___/ _ \
    / /___/ /_/ (__  |__  ) /_/ / / / / /_/ / /  / /_/ /___/ / /_/ / /_/ / /  / /__/  __/
    \____/\__,_/____/____/\__,_/_/ /_/\__,_/_/   \__,_//____/\____/\__,_/_/   \___/\___/

 By Andrew Stevenson. (com.datamountaineer.streamreactor.connect.cassandra.source.CassandraSourceTask:64)
[2016-05-06 13:34:41,193] INFO Attempting to connect to Cassandra cluster at localhost and create keyspace demo. (com.datamountaineer.streamreactor.connect.cassandra.CassandraConnection$:49)
[2016-05-06 13:34:41,263] INFO Using username_password. (com.datamountaineer.streamreactor.connect.cassandra.CassandraConnection$:83)
[2016-05-06 13:34:41,459] INFO Did not find Netty's native epoll transport in the classpath, defaulting to NIO. (com.datastax.driver.core.NettyUtil:83)
[2016-05-06 13:34:41,711] WARN You listed localhost/0:0:0:0:0:0:0:1:9042 in your contact points, but it wasn't found in the control host's system.peers at startup (com.datastax.driver.core.Cluster:2105)
[2016-05-06 13:34:41,823] INFO Using data-center name 'datacenter1' for DCAwareRoundRobinPolicy (if this is incorrect, please provide the correct datacenter name with DCAwareRoundRobinPolicy constructor) (com.datastax.driver.core.policies.DCAwareRoundRobinPolicy:95)
[2016-05-06 13:34:41,824] INFO New Cassandra host localhost/127.0.0.1:9042 added (com.datastax.driver.core.Cluster:1475)
[2016-05-06 13:34:41,868] INFO Connection to Cassandra established. (com.datamountaineer.streamreactor.connect.cassandra.source.CassandraSourceTask:87)
[2016-05-06 13:34:41,878] INFO Received setting:
 	keySpace: demo
	table: orders
	topic: orders-topic
	importMode: false
	timestampColumn: created
	allowFiltering: true (com.datamountaineer.streamreactor.connect.cassandra.source.CassandraTableReader:48)
[2016-05-06 13:34:41,923] INFO Source task Thread[WorkerSourceTask-cassandra-source-orders-0,5,main] finished initialization and start (org.apache.kafka.connect.runtime.WorkerSourceTask:342)
[2016-05-06 13:34:41,927] INFO Query SELECT * FROM demo.orders WHERE created > maxTimeuuid(?) AND created <= minTimeuuid(?)  ALLOW FILTERING executing with bindings (1900-01-01 00:19:32+0019, 2016-05-06 13:34:41+0200). (com.datamountaineer.streamreactor.connect.cassandra.source.CassandraTableReader:156)
[2016-05-06 13:34:41,948] INFO Querying returning results for demo.orders. (com.datamountaineer.streamreactor.connect.cassandra.source.CassandraTableReader:185)
[2016-05-06 13:34:41,958] INFO Found 3. Draining entries to batchSize 100. (com.datamountaineer.streamreactor.connect.queues.QueueHelpers$:45)
[2016-05-06 13:34:41,958] INFO Processed 3 rows for table orders-topic.orders (com.datamountaineer.streamreactor.connect.cassandra.source.CassandraTableReader:206)
```

We can then use the kafka-avro-console-consumer to see what's in the kafka topic we have routed the order table to.

```bash
➜  confluent-2.0.1/bin/kafka-avro-console-consumer --zookeeper localhost:2181 --topic orders-topic --from-beginning 
{"id":{"int":1},"created":{"string":"17fa1050-137e-11e6-ab60-c9fbe0223a8f"},"price":{"float":94.2},"product":{"string":"OP-DAX-P-20150201-95.7"},"qty":{"int":100}}
{"id":{"int":2},"created":{"string":"17fb6fe0-137e-11e6-ab60-c9fbe0223a8f"},"price":{"float":99.5},"product":{"string":"OP-DAX-C-20150201-100"},"qty":{"int":100}}
{"id":{"int":3},"created":{"string":"17fbbe00-137e-11e6-ab60-c9fbe0223a8f"},"price":{"float":150.0},"product":{"string":"FU-KOSPI-C-20150201-100"},"qty":{"int":200}}
```
3 row as expected. 

Now stop the connector.

.. note:: Next time the Connector polls another 3 would be pulled in. In our example the default poll interval is set to 1 minute. So in 1 minute we'd get rows again.

.. note:: The created field in a TimeUUID is Cassandra, this represented as a string in the Kafka Connect schema.

#### Source Connector Configuration (Incremental)

The configuration is similar to before but this time well perform an incremental load. Below is the configuration. Create a file called cassandra-source-distributed-orders.properties and add the following content:

```bash
name=cassandra-source-orders
connector.class=com.datamountaineer.streamreactor.connect.cassandra.source.CassandraSourceConnector
cassandra.key.space=demo
cassandra.import.map=orders:orders-topic
cassandra.import.timestamp.column=orders:created
cassandra.import.mode=incremental
cassandra.authentication.mode=username_password
cassandra.contact.points=localhost
cassandra.username=cassandra
cassandra.password=cassandra
```

There are two changes from the previous configuration:

1. *cassandra.import.timestamp.column* has been added to identify the column used in the where clause with the lower and upper bounds.
2. The *cassandra.import.mode* has been set to *incremental*.

.note::Only Cassandra columns with data type Timeuuid are supported for incremental mode. The column must also be either the primary key or part of the compound key. If it's part of the compound key this will introduce a full scan with ALLOW_FILTERING added to the query.

We can reuse the 3 records inserted into Cassandra earlier but lets clean out the target Kafka topic. 

.note::You must delete.topics.enable in etc/kafka/server.properties and shutdown any consumers of this topic for this to take effect.

```bash
#Delete the topic
➜  confluent-2.0.1/bin/kafka-topics --zookeeper localhost:2181 --topic orders-topic --delete
```

#### Starting the Connector (Distributed)

Connectors can be deployed distributed mode. In this mode one or many connectors are started on the same or different hosts with the same cluster id. The cluster id can be found in etc/schema-registry/connect-avro-distributed.properties.

```
# The group ID is a unique identifier for the set of workers that form a single Kafka Connect
# cluster
group.id=connect-cluster
```

For this quick-start we will just use one host. 

Now start the connector in distributed mode, this time we only give it one properties file for the kafka, zookeeper and schema registry configurations.

```bash
➜  confluent-2.0.1/bin/connect-distributed etc/schema-registry/connect-avro-distributed.properties 
```

Once the connector has started lets use the kafka-connect-tools cli to post in our distributed properties file.

```bash
➜  java -jar build/libs/kafka-connect-cli-0.2-all.jar create cassandra-source-orders < cassandra-source-distributed-orders.properties 

#Connector `cassandra-source-orders`:
cassandra.key.space=demo
name=cassandra-source-orders
cassandra.import.mode=incremental
connector.class=com.datamountaineer.streamreactor.connect.cassandra.source.CassandraSourceConnector
cassandra.authentication.mode=username_password
cassandra.contact.points=localhost
cassandra.username=cassandra
cassandra.password=cassandra
cassandra.import.map=orders:orders-topic
cassandra.import.timestamp.column=orders:created
#task ids: 0
```

If you switch back to the terminal you started the Connector in you should see the Cassandra Source being accepted and the task starting and processing the 3 existing rows.

```
[2016-05-06 13:44:32,963] INFO Received setting:
 	keySpace: demo
	table: orders
	topic: orders-topic
	importMode: false
	timestampColumn: created
	allowFiltering: true (com.datamountaineer.streamreactor.connect.cassandra.source.CassandraTableReader:48)
[2016-05-06 13:44:33,132] INFO Source task Thread[WorkerSourceTask-cassandra-source-orders-0,5,main] finished initialization and start (org.apache.kafka.connect.runtime.WorkerSourceTask:342)
[2016-05-06 13:44:33,137] INFO Query SELECT * FROM demo.orders WHERE created > maxTimeuuid(?) AND created <= minTimeuuid(?)  ALLOW FILTERING executing with bindings (2016-05-06 09:23:28+0200, 2016-05-06 13:44:33+0200). (com.datamountaineer.streamreactor.connect.cassandra.source.CassandraTableReader:156)
[2016-05-06 13:44:33,151] INFO Querying returning results for demo.orders. (com.datamountaineer.streamreactor.connect.cassandra.source.CassandraTableReader:185)
[2016-05-06 13:44:33,160] INFO Processed 3 rows for table orders-topic.orders (com.datamountaineer.streamreactor.connect.cassandra.source.CassandraTableReader:206)
[2016-05-06 13:44:33,160] INFO Found 3. Draining entries to batchSize 100. (com.datamountaineer.streamreactor.connect.queues.QueueHelpers$:45)
[2016-05-06 13:44:33,197] WARN Error while fetching metadata with correlation id 0 : {orders-topic=LEADER_NOT_AVAILABLE} (org.apache.kafka.clients.NetworkClient:582)
[2016-05-06 13:44:33,406] INFO Found 0. Draining entries to batchSize 100. (com.datamountaineer.streamreactor.connect.queues.QueueHelpers$:45)
```

Check Kafka, 3 rows as before.

```bash
➜  confluent-2.0.1/bin/kafka-avro-console-consumer --zookeeper localhost:2181 --topic orders-topic --from-beginning 
{"id":{"int":1},"created":{"string":"Thu May 05 13:24:22 CEST 2016"},"price":{"float":94.2},"product":{"string":"DAX-P-20150201-95.7"},"qty":{"int":100}}
{"id":{"int":2},"created":{"string":"Thu May 05 13:26:21 CEST 2016"},"price":{"float":99.5},"product":{"string":"OP-DAX-C-20150201-100"},"qty":{"int":100}}
{"id":{"int":3},"created":{"string":"Thu May 05 13:26:44 CEST 2016"},"price":{"float":150.0},"product":{"string":"FU-KOSPI-C-20150201-100"},"qty":{"int":200}}
```

The source tasks will continue to poll but not pick up any new rows yet.

```
INFO Query SELECT * FROM demo.orders WHERE created > ? AND created <= ?  ALLOW FILTERING executing with bindings (Thu May 05 13:26:44 CEST 2016, Thu May 05 21:19:38 CEST 2016). (com.datamountaineer.streamreactor.connect.cassandra.source.CassandraTableReader:152)
INFO Querying returning results for demo.orders. (com.datamountaineer.streamreactor.connect.cassandra.source.CassandraTableReader:181)
INFO Processed 0 rows for table orders-topic.orders (com.datamountaineer.streamreactor.connect.cassandra.source.CassandraTableReader:202)
```

##### Inserting new data

Now lets insert a row into the Cassandra table. Start the CQL shell.

```bash
➜  bin ./cqlsh
Connected to Test Cluster at 127.0.0.1:9042.
[cqlsh 5.0.1 | Cassandra 3.0.2 | CQL spec 3.3.1 | Native protocol v4]
Use HELP for help.

```

Execute the following:

```bash
use demo;

INSERT INTO orders (id, created, product, qty, price) VALUES (4, now(), 'FU-DATAMOUNTAINEER-C-20150201-100', 500, 10000);

SELECT * FROM orders;

 id | created                              | price | product                           | qty
----+--------------------------------------+-------+-----------------------------------+-----
  1 | 17fa1050-137e-11e6-ab60-c9fbe0223a8f |  94.2 |            OP-DAX-P-20150201-95.7 | 100
  2 | 17fb6fe0-137e-11e6-ab60-c9fbe0223a8f |  99.5 |             OP-DAX-C-20150201-100 | 100
  4 | 02acf5d0-1380-11e6-ab60-c9fbe0223a8f | 10000 | FU-DATAMOUNTAINEER-C-20150201-100 | 500
  3 | 17fbbe00-137e-11e6-ab60-c9fbe0223a8f |   150 |           FU-KOSPI-C-20150201-100 | 200

(4 rows)
cqlsh:demo> 
```

Check the logs.

```
[2016-05-06 13:45:33,134] INFO Query SELECT * FROM demo.orders WHERE created > maxTimeuuid(?) AND created <= minTimeuuid(?)  ALLOW FILTERING executing with bindings (2016-05-06 13:31:37+0200, 2016-05-06 13:45:33+0200). (com.datamountaineer.streamreactor.connect.cassandra.source.CassandraTableReader:156)
[2016-05-06 13:45:33,137] INFO Querying returning results for demo.orders. (com.datamountaineer.streamreactor.connect.cassandra.source.CassandraTableReader:185)
[2016-05-06 13:45:33,138] INFO Processed 1 rows for table orders-topic.orders (com.datamountaineer.streamreactor.connect.cassandra.source.CassandraTableReader:206)
[2016-05-06 13:45:33,138] INFO Found 0. Draining entries to batchSize 100. (com.datamountaineer.streamreactor.connect.queues.QueueHelpers$:45)

```

Check Kafka.

```bash
➜  confluent confluent-2.0.1/bin/kafka-avro-console-consumer --zookeeper localhost:2181 --topic orders-topic --from-beginning
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
{"id":{"int":1},"created":{"string":"17fa1050-137e-11e6-ab60-c9fbe0223a8f"},"price":{"float":94.2},"product":{"string":"OP-DAX-P-20150201-95.7"},"qty":{"int":100}}
{"id":{"int":2},"created":{"string":"17fb6fe0-137e-11e6-ab60-c9fbe0223a8f"},"price":{"float":99.5},"product":{"string":"OP-DAX-C-20150201-100"},"qty":{"int":100}}
{"id":{"int":3},"created":{"string":"17fbbe00-137e-11e6-ab60-c9fbe0223a8f"},"price":{"float":150.0},"product":{"string":"FU-KOSPI-C-20150201-100"},"qty":{"int":200}}
{"id":{"int":4},"created":{"string":"02acf5d0-1380-11e6-ab60-c9fbe0223a8f"},"price":{"float":10000.0},"product":{"string":"FU-DATAMOUNTAINEER-C-20150201-100"},"qty":{"int":500}}
```

Bingo, we have our extra row.

## Sink Connector QuickStart

The Cassandra Sink allows you to write events from Kafka to Cassandra. The connector converts the value from the Kafka Connect SinkRecords to Json and uses Cassandra's JSON insert functionality to insert the rows.

The task expects pre-created tables in Cassandra. Like the source connector the sink allows mapping of topics to tables.

.note:: The table and keyspace must be created before hand!
.note:: If the target table has TimeUUID fields the payload string for the corresponding field in Kafka must be a UUID.

For the quick-start we will reuse the order-topic we created for the source.

### Sink Connector Configuration

The sink configuration is similar to the source, they share most of the same configuration options. Create a file called cassandra-sink-distributed-orders.properties with contents below.

```bash 
name=cassandra-sink-orders
connector.class=com.datamountaineer.streamreactor.connect.cassandra.sink.CassandraSinkConnector
tasks.max=1
topics=orders-topic 
connect.cassandra.export.map=orders-topic:orders_write_back
connect.cassandra.contact.points=localhost
connect.cassandra.port=9042
connect.cassandra.key.space=demo
connect.cassandra.authentication.mode=username_password
connect.cassandra.contact.points=localhost
connect.cassandra.username=cassandra
connect.cassandra.password=cassandra
```

The main difference here is the *cassandra.export.map*. This like the source connector but reversed is comma separated list of topic to table mappings. The mapping for each element in the list is separate by a colon. The topic is before and the table after the colon. In this example the routing is orders-topic to the orders\_write\_back table in Cassandra.

Additional we must supply the topics configuration option. 

.note::All tables must be in the same keyspace.

.note::If a topic specified in the topics configuration option is not present in the export.map the the topic name will be used.

### Cassandra Tables

The sink expects the tables it's configured to write to are already present in Cassandra. Lets create our table for the sink.

```bash

use demo;
create table orders_write_back (id int, created timeuuid, product text, qty int, price float, PRIMARY KEY (id, created)) WITH CLUSTERING ORDER BY (created asc);
SELECT * FROM orders_write_back;

 id | created | price | product | qty
----+---------+-------+---------+-----

(0 rows)
cqlsh:demo> 
```

### Starting the Sink Connector (Distributed)

Again will start in distributed mode.

```bash
➜  confluent-2.0.1/bin/connect-distributed etc/schema-registry/connect-avro-distributed.properties 
```

Once the connector has started lets use the kafka-connect-tools cli to post in our distributed properties file.


```bash
➜  java -jar build/libs/kafka-connect-cli-0.3-all.jar create cassandra-sink-orders < cassandra-sink-distributed-orders.properties 

#Connector `cassandra-sink-orders`:
name=cassandra-sink-orders
connector.class=com.datamountaineer.streamreactor.connect.cassandra.sink.CassandraSinkConnector
tasks.max=1
topics=orders-topic
connect.cassandra.export.map=orders-topic:orders_write_back
connect.cassandra.contact.points=localhost
connect.cassandra.port=9042
connect.cassandra.key.space=demo
connect.cassandra.authentication.mode=username_password
connect.cassandra.contact.points=localhost
connect.cassandra.username=cassandra
connect.cassandra.password=cassandra
#task ids: 0
```

Now check the logs to see if we started the sink.

```
[2016-05-06 13:52:28,178] INFO 
    ____        __        __  ___                  __        _
   / __ \____ _/ /_____ _/  |/  /___  __  ______  / /_____ _(_)___  ___  ___  _____
  / / / / __ `/ __/ __ `/ /|_/ / __ \/ / / / __ \/ __/ __ `/ / __ \/ _ \/ _ \/ ___/
 / /_/ / /_/ / /_/ /_/ / /  / / /_/ / /_/ / / / / /_/ /_/ / / / / /  __/  __/ /
/_____/\__,_/\__/\__,_/_/  /_/\____/\__,_/_/ /_/\__/\__,_/_/_/ /_/\___/\___/_/
       ______                                __           _____ _       __
      / ____/___ _______________ _____  ____/ /________ _/ ___/(_)___  / /__
     / /   / __ `/ ___/ ___/ __ `/ __ \/ __  / ___/ __ `/\__ \/ / __ \/ //_/
    / /___/ /_/ (__  |__  ) /_/ / / / / /_/ / /  / /_/ /___/ / / / / / ,<
    \____/\__,_/____/____/\__,_/_/ /_/\__,_/_/   \__,_//____/_/_/ /_/_/|_|

 By Andrew Stevenson. (com.datamountaineer.streamreactor.connect.cassandra.sink.CassandraSinkTask:50)
[2016-05-06 13:52:28,179] INFO Attempting to connect to Cassandra cluster at localhost and create keyspace demo. (com.datamountaineer.streamreactor.connect.cassandra.CassandraConnection$:49)
[2016-05-06 13:52:28,179] INFO Using username_password. (com.datamountaineer.streamreactor.connect.cassandra.CassandraConnection$:83)
[2016-05-06 13:52:28,187] WARN You listed localhost/0:0:0:0:0:0:0:1:9042 in your contact points, but it wasn't found in the control host's system.peers at startup (com.datastax.driver.core.Cluster:2105)
[2016-05-06 13:52:28,211] INFO Using data-center name 'datacenter1' for DCAwareRoundRobinPolicy (if this is incorrect, please provide the correct datacenter name with DCAwareRoundRobinPolicy constructor) (com.datastax.driver.core.policies.DCAwareRoundRobinPolicy:95)
[2016-05-06 13:52:28,211] INFO New Cassandra host localhost/127.0.0.1:9042 added (com.datastax.driver.core.Cluster:1475)
[2016-05-06 13:52:28,290] INFO Initialising Cassandra writer. (com.datamountaineer.streamreactor.connect.cassandra.sink.CassandraJsonWriter:40)
[2016-05-06 13:52:28,295] INFO Preparing statements for orders-topic. (com.datamountaineer.streamreactor.connect.cassandra.sink.CassandraJsonWriter:62)
[2016-05-06 13:52:28,305] INFO Sink task org.apache.kafka.connect.runtime.WorkerSinkTask@37e65d57 finished initialization and start (org.apache.kafka.connect.runtime.WorkerSinkTask:155)
[2016-05-06 13:52:28,331] INFO Source task Thread[WorkerSourceTask-cassandra-source-orders-0,5,main] finished initialization and start (org.apache.kafka.connect.runtime.WorkerSourceTask:342)
```

Now check Cassandra

```bash
use demo;
SELECT * FROM orders_write_back;

 id | created                              | price | product                           | qty
----+--------------------------------------+-------+-----------------------------------+-----
  1 | 17fa1050-137e-11e6-ab60-c9fbe0223a8f |  94.2 |            OP-DAX-P-20150201-95.7 | 100
  2 | 17fb6fe0-137e-11e6-ab60-c9fbe0223a8f |  99.5 |             OP-DAX-C-20150201-100 | 100
  4 | 02acf5d0-1380-11e6-ab60-c9fbe0223a8f | 10000 | FU-DATAMOUNTAINEER-C-20150201-100 | 500
  3 | 17fbbe00-137e-11e6-ab60-c9fbe0223a8f |   150 |           FU-KOSPI-C-20150201-100 | 200

(4 rows)
```

Bingo, our 4 rows!


## Features

Both the source and sink connector use Cassandra's executeAysnc function. This is non blocking. For the source, the when the result returns it is iterated over and rows added to a internal queue. This queue is then drained by the connector and written to Kafka. 

### Source Connector

#### Data Types

The source connector supports copying tables in bulk and incrementally to Kafka.

The following CQL data types are supported:

CQL Type | Connect Data Type
---------|------------------
TimeUUID | Optional String
UUID | Optional String
Inet | Optional String
Ascii | Optional String
Text | Optional String
Timestamp | Optional String
Date | Optional String
Tuple | Optional String
UDT | Optional String
Boolean | Optional Boolean
TinyInt | Optional Int8
SmallInt | Optional Int16
Int | Optional Int32
Decimal | Optional String
Float | Optional Float32
Counter | Optional Int64
BigInt | Optional Int64
VarInt | Optional Int64
Double | Optional Int64
Time  | Optional Int64
Blob | Optional Bytes
Map | Optional String
List | Optional String
Set | Optional String

.note:: For Map, List and Set the value is extracted from the Cassandra Row and inserted as a JSON string representation.

#### Modes

The source connector runs in both bulk and incremental mode.

Each mode has a polling interval. This interval determines how often the readers execute queries against the Cassandra tables. It applies to both incremental and bulk modes. The ``cassandra.import.mode`` setting controls the import behaviour.

##### Incremental

In ``incremental`` mode the connector supports querying based on a column in the tables with CQL data type of TimeUUID.

Kafka Connect tracks the latest record it retrieved from each table, so it can start at the correct location on the next iteration (or in case of a crash). In this case the maximum value of the records returned by the result-set is tracked and stored in Kafka by the framework. If no offset is found for the table at startup a default timestamp of 1900-01-01 is used. This is then passed to a prepared statement containing a range query.

.e.g

```sql
SELECT * FROM demo.orders WHERE created > maxTimeuuid(?) AND created <= minTimeuuid(?)
```

.note:: ! If the column used for tracking timestamps is a compound key, ALLOW FILTERING is appended to the query. This can have a detrimental performance impact of Cassandra as it is effectively issuing a full scan.

##### Bulk

In ``bulk`` mode the connector extracts the full table, no where clause is attached to the query.

.note:: ! Watch out with the poll interval. After each interval the bulk query will be executed again. 

#### Mappings

The source connector supports mapping of tables to columns. This is controlled via the ``cassandra.import.table.map`` configuration option. This option expects a comma separated list of mappings of table to topic, separated by a colon. If no topic is provided the table name is used.

### Sink Connector

The sink connector uses Cassandra's [JSON](http://www.datastax.com/dev/blog/whats-new-in-cassandra-2-2-json-support) insert functionality.

The SinkRecord from Kafka connect is converted to JSON and feed into the prepared statements for inserting into Cassandra.

See DataStax's [documentation](http://cassandra.apache.org/doc/cql3/CQL-2.2.html#insertJson) for type mapping.

#### Mappings

The sink connector supports mapping of topics to tables. This is controlled via the ``cassandra.export.topic.table.map`` configuration option. This option expects a comma separated list of mappings of topic to table, separated by a colon. If no table is provided the topic name is used.

## Configurations

Configurations common to both sink and source are:

name | data type | required | description
-----|-----------|----------|------------
connect.cassandra.contact.points | string | yes | contact points (hosts) in Cassandra cluster
connect.cassandra.key.space | string | yes | key_space the tables to write to belong to
connect.cassandra.port | int | no | port for the native Java driver (default 9042)
connect.cassandra.authentication.mode | string | no | Mode to authenticate with Cassandra, either username or none, default is none
connect.cassandra.username | no | string | Username to connect to Cassandra with if USERNAME_PASSWORD enabled
connect.cassandra.password | no | string | Password to connect to Cassandra with if USERNAME_PASSWORD enabled
connect.cassandra.ssl.enabled | no | boolean | Enables SSL communication against SSL enabled Cassandra, default false
connect.cassandra.trust.store.path | no | string | Path to truststore
connect.cassandra.trust.store.password | no | string | Password for truststore
connect.cassandra.key.store.path | no | string | Path to keystore
connect.cassandra.key.store.password | no | string | Password for the keystore
connect.cassandra.ssl.client.cert.aut | no | boolean | Enable client certification authentication by Cassandra. Requires KeyStore options to be set. Default false.

### Source Connector Configurations

Configurations options specific to the source connector are:

name | data type | required | description
-----|-----------|----------|------------
connect.cassandra.import.poll.interval | int | no | The polling interval between queries against tables for bulk mode in milliseconds. Default is 1 minute. **WATCH OUT WITH BULK MODE AS MAY REPEATEDLY PULL IN THE SAME DATE.**
connect.cassandra.import.mode | string | yes | Either bulk or incremental
connect.cassandra.import.timestamp.column | string | yes | Name of the timestamp column in the cassandra table to use identify deltas. table1:col,table2:col. **MUST BE OF TYPE TIMEUUID**
connect.cassandra.import.table.map | string | yes | Table to Topic map for import in format table1=topic1,table2=topic2, if the topic left blank table name is used
connect.cassandra.import.source.allow.filtering | string | no | Enable ALLOW FILTERING in incremental selects. Default is true
connect.cassandra.import.fetch.size | int | no | The fetch size for the Cassandra driver to read. Default is 1000.
connect.cassandra.source.task.buffer.size | int | no | The size of the queue as read writes to. Default 10000.
connect.cassandra.source.task.batch.size | int | no | The number of records the source task should drain from the reader queue.

#### Bulk Example

```bash
name=cassandra-source-orders-bulk
connector.class=com.datamountaineer.streamreactor.connect.cassandra.source.CassandraSourceConnector
connect.cassandra.key.space=demo
connect.cassandra.import.map=orders:orders-topic
connect.cassandra.import.mode=bulk
connect.cassandra.authentication.mode=username_password
connect.cassandra.contact.points=localhost
connect.cassandra.username=cassandra
connect.cassandra.password=cassandra
```

#### Incremental Example

```bash
name=cassandra-source-orders-incremental
connector.class=com.datamountaineer.streamreactor.connect.cassandra.source.CassandraSourceConnector
connect.cassandra.key.space=demo
connect.cassandra.import.map=orders:orders-topic
connect.cassandra.import.timestamp.column=orders:created
connect.cassandra.import.mode=incremental
connect.cassandra.authentication.mode=username_password
connect.cassandra.contact.points=localhost
connect.cassandra.username=cassandra
connect.cassandra.password=cassandra
```

### Sink Connector Configurations

Configurations options specific to the sink connector are:

name | data type | required | description
-----|-----------|----------|------------
connect.cassandra.export.map | string | yes | Topic to Table map for import in format topic1:table1, if the table left blank topic name is used.

#### Example

```bash
name=cassandra-sink-orders
connector.class=com.datamountaineer.streamreactor.connect.cassandra.sink.CassandraSinkConnector
tasks.max=1
topics=orders-topic
connect.cassandra.export.map=orders-topic:orders_write_back
connect.cassandra.contact.points=localhost
connect.cassandra.port=9042
connect.cassandra.key.space=demo
connect.cassandra.authentication.mode=username_password
connect.cassandra.contact.points=localhost
connect.cassandra.username=cassandra
connect.cassandra.password=cassandra
```

## Schema Evolution

TODO

## Deployment Guidelines

TODO

## TroubleShooting

TODO


