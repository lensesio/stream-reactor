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

#### [Kafka-Connect-Bloomberg](http://docs.datamountaineer.com/en/latest/bloomberg.html)

Kafka connect Bloomberg source to subscribe to Bloomberg streams via the open API and write to Kafka.

#### [Kafka-Connect-Cassandra-Source](http://docs.datamountaineer.com/en/latest/cassandra-source.html)

Kafka connect Cassandra source to read Cassandra and write to Kafka.

#### [Kafka-Connect-Cassandra-Sink](http://docs.datamountaineer.com/en/latest/cassandra-sink.html)

Kafka connect Cassandra sink task to write Kafka topic payloads to Cassandra.

#### [Kafka-Connect-Druid](http://docs.datamountaineer.com/en/latest/druid.html)

Kafka connect Druid sink to write Kafka topic payloads to Druid.

WORK IN PROGRESS

#### [Kafka-Connect-Elastic](http://docs.datamountaineer.com/en/latest/elastic.html)

Kafka connect Elastic Search sink to write Kafka topic payloads to Elastic Search.

#### [Kafka-Connect-HBase](http://docs.datamountaineer.com/en/latest/hbase.html)

Kafka connect HBase sink to write Kafka topic payloads to HBase.

#### [Kafka-Connect-Hazelcast](http://docs.datamountaineer.com/en/latest/hazelcast.html)

Kafka connect HBase sink to write Kafka topic payloads to Hazelcast.

#### [Kafka-Connect-Kudu](http://docs.datamountaineer.com/en/latest/kudu.html)

Kafka connect Kudu sink to write Kafka topic payloads to Kudu.

#### [Kafka-Connect-InfluxDb](http://docs.datamountaineer.com/en/latest/influx.html)

Kafka connect InfluxDb sink to write Kafka topic payloads to InfluxDb.

#### [Kafka-Connect-JMS](http://docs.datamountaineer.com/en/latest/jms.html)

Kafka connect JMS sink to write Kafka topic payloads to JMS.

#### [Kafka-Connect-Redis](http://docs.datamountaineer.com/en/latest/redis.html)

Kafka connect Redis sink to write Kafka topic payloads to Redis.

#### [Kafka-Connect-Rethink](http://docs.datamountaineer.com/en/latest/rethink.html)

Kafka connect RethinkDb sink to write Kafka topic payloads to RethinkDb.

#### [Kafka-Connect-Yahoo](http://docs.datamountaineer.com/en/latest/yahoo.html)

Kafka connect Yahoo Finance source to write to Kafka

#### [Kafka-Connect-Voltdb](http://docs.datamountaineer.com/en/latest/voltdb.html)

Kafka connect Voltdb sink to write Kafka topic payloads to Votldb.

#### [Kafka-Socket-Streamer](kafka-socket-streamer/README.md)

Akka Http and Reactive Kafka with Websocket and Server Send Event support.
Supports limited SQL statements to stream and select from Kafka topics.
