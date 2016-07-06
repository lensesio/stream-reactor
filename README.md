[![Stories in Ready](https://badge.waffle.io/datamountaineer/stream-reactor.png?label=ready&title=Ready)](https://waffle.io/datamountaineer/stream-reactor)
[![Build Status](https://datamountaineer.ci.landoop.com/buildStatus/icon?job=stream-reactor&style=flat&.png)](https://datamountaineer.ci.landoop.com/job/stream-reactor/)
[![Documentation Status](https://readthedocs.org/projects/streamreactor/badge/?version=latest)](http://docs.datamountaineer.com/en/latest/?badge=latest)

<!--[![Build Status](https://travis-ci.org/datamountaineer/stream-reactor.svg?branch=master)](https://travis-ci.org/datamountaineer/stream-reactor)--> 
![](images/DM-logo.jpg)

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
```

or with no tests run

```
gradle fatJarNoTest
```

You can also use the gradle wrapper

```
./gradlew fatJar
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

#### [Kafka-Connect-HBase](kafka-connect-hbase/README.md)

Kafka connect HBase sink to write Kafka topic payloads to HBase.

#### [Kafka-Connect-Redis](kafka-connect-redis/README.md)

Kafka connect Redis sink to write Kafka topic payloads to Redis.

#### [Kafka-Connect-Rethink](kafka-connect-redis/README.md)

Kafka connect RethinkDb sink to write Kafka topic payloads to RethinkDb.

#### [Kafka-Socket-Streamer](kafka-socket-streamer/README.md)

Test module for Akka Http and Reactive Kafka with Websocket and Server Send Event support.
