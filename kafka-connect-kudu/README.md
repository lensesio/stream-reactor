![](../images/DM-logo.jpg)
[![Documentation Status](https://readthedocs.org/projects/streamreactor/badge/?version=latest)](http://streamreactor.readthedocs.io/en/latest/kudu.html#kafka-connect-kudu)

# Kafka Connect Kudu

A Connector and Sink to write events from Kafka to Kudu. 

Assumes SinkRecord schema matches Kudu table schema. Kudu tables must be precreated.

Writing with the same primary key twice will result in a Kudu write failure. See [this](http://getkudu.io/docs/kudu_impala_integration.html#impala_insertion_caveat)

## Prerequisites
* Confluent 3.0.0
* Kudu 0.9
* Java 1.8 
* Scala 2.11

##Build

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

See the documentation for more information.
