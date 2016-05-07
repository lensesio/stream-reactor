[![Build Status](https://travis-ci.org/datamountaineer/stream-reactor-gradle.svg?branch=master)](https://travis-ci.org/datamountaineer/stream-reactor-gradle)
[![Documentation Status](https://readthedocs.org/projects/streamreactor/badge/?version=latest)](http://streamreactor.readthedocs.org/en/latest/?badge=latest)

![](../images/DM-logo.jpg)

# Stream Reactor
Streaming reference architecture built around Kafka. 

![Alt text](https://datamountaineer.files.wordpress.com/2016/01/stream-reactor-1.jpg?w=1320)

A collection of components to build a real time ingestion pipeline.

Publication to Maven coming soon.

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

#### [Kafka-Connect-Cassandra](kafka-connect-cassandra/README.md)

Kafka connect Cassandra source to read Cassandra and write to Kafka

Kafka connect Cassandra sink task to write Kafka topic payloads to Cassandra.

#### [Kafka-Connect-Elastic](kafka-connect-elastic/README.md)

Kafka connect Elastic Search sink to write Kafka topic payloads to Elastic Search.

#### [Kafka-Connect-Kudu](kafka-connect-kudu/README.md)

Kafka connect Kudu sink to write Kafka topic payloads to Kudu.

#### [Kafka-Connect-Bloomberg](kafka-connect-bloomberg/README.md)

Kafka connect Bloomberg source to subscribe to Bloomberg streams via the open API and write to Kafka.

#### [Kafka-Connect-Druid](kafka-connect-druid/README.md)

Kafka connect Druid sink to write Kafka topic payloads to Druid.

#### [Kafka-Connect-HBase](kafka-connect-hbase)/README.md)

Kafka connect HBase sink to write Kafka topic payloads to HBase.

#### [Kafka-Connect-Redis](kafka-connect-redis)/README.md)

Kafka connect HBase sink to write Kafka topic payloads to HBase.

#### [Kafka-Socket-Streamer](kafka-socket-streamer/README.md)

Test module for Akka Http and Reactive Kafka with Websocket and Server Send Event support.

#### Future Work

* HBase Source
* Kudu Source
* Redis Source
* Druid Source
* Elastic Source
* OrientDb
* Neo4j
* Rest (spray-can post)
* UPC Scada Source and Sink
* MQTT Sink
* RDBMS (Sql Server 2016 supports JSON inserts)
* TCP sockets?
* Solr
* UDP
* Websockets/Server Send Events
* Couchbase
* Hive
* JMS
