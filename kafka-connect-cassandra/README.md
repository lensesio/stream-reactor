![](../images/DM-logo.jpg)
[![Documentation Status](http://docs.datamountaineer.com/en/latest/)](http://docs.datamountaineer.com/en/latest/?badge=latest)


# Kafka Connect Cassandra

A Connector and Sink to write events from Kafka to Cassandra. 

**Please go to the documentation for Usage.**

## Perquisites
* Cassandra 2.2.4
* Confluent 2.0.1
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



