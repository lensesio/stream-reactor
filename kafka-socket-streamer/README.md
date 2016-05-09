![](../images/DM-logo.jpg)

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

## Perquisites

* Confluent 2.0


## Testing Server Send Events

```bash
 curl 'http://localhost:8080/sse/topics?topic=_schemas&consumergroup=testcg'
```

## Testing Websocket

Can be tested with Postman for example, if using postman added a extra space at the end

```bash
 http://localhost:8080/ws/topics?topic=_schemas&consumergroup=testcg'
```
