![](../images/DM-logo.jpg)
[![Documentation Status](https://readthedocs.org/projects/streamreactor/badge/?version=latest)](http://streamreactor.readthedocs.io/en/latest/socket-streamer.html#kafka-socket-streamer)


# Kafka Socket Streamer

Akka Http with Reactive Kafka to stream topics to clients via Web sockets and Server Send Events.

This is test and not yet intended for any serious use yet.

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

## Prerequisites

* Confluent 2.0.1

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