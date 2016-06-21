![](../images/DM-logo.jpg)
[![Documentation Status](https://readthedocs.org/projects/streamreactor/badge/?version=latest)](http://streamreactor.readthedocs.io/en/latest/bloomberg.html#kafka-connect-bloomberg)

# Kafka Connect Bloomberg

A Connector to Subscribe to Bloomberg ticker updates and write the data to Kafka. The connector will create and open a
Bloomberg session and will subscribe for data updates given the configuration provided. As data is received from Bloomberg
it will be added to a blocking queue. When the kafka connect framework polls the source task for data it will drain this
queue.

When the session is created it will be linked to a Bloomberg service. There are only two supported: "//blp/mkdata" and
"//blp/refdata".

The configuration can specify different tickers to subscribe for and for each one it can specify a different fields. See in the Properties section more details

## Build

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