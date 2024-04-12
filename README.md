![Actions Status](https://github.com/lensesio/stream-reactor/actions/workflows/build.yml/badge.svg)
[<img src="https://img.shields.io/badge/docs--orange.svg?"/>](https://docs.lenses.io/connectors/)

Join us on slack [![Alt text](images/slack.jpeg)](https://launchpass.com/lensesio)

# Lenses Connectors for Apache Kafka

Lenses.io is the leader in offering Apache 2 licensed Kafka Connectors (Stream Reactor) since 2016. 

## Enterprise Support for Kafka Connectors 

Lenses offers the leading Developer Experience solution for engineers building real-time applications on any Apache Kafka ([lenses.io](http://www.lenses.io)). Subscribed customers are entitled to full 24x7 support for selected Kafka Connectors. This includes priority over feature requests and security incident SLAs. Email info@lenses.io for more information. 

## Engage with the Community

Speak to us on our Community Slack channel (Register at https://launchpass.com/lensesio) or ask the Community a question in our [Ask Marios](http://www.lenses.io) forum. 

## Kafka Connectors Roadmap

A series of next-generation Connectors are in active development. Give us your feedback of which connectors we should be working on or to to get the latest information, send us an email at info@lenses.io

# Stream Reactor Kafka Connectors

![Alt text](images/streamreactor-logo.png)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Flensesio%2Fstream-reactor.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Flensesio%2Fstream-reactor?ref=badge_shield)

A collection of components to build a real time ingestion pipeline.

## Kafka Compatibility

* Kafka 2.8 -> 3.5 (Confluent 6.2 -> 7.5) - Stream Reactor 4.1.0+
* Kafka 3.1 (Confluent 7.1) - Stream Reactor 4.0.0 (Kafka 3.1 Build)
* Kafka 2.8 (Confluent 6.2) - Stream Reactor 4.0.0 (Kafka 2.8 Build)
* Kafka 2.5 (Confluent 5.5) - Stream reactor 2.0.0+
* Kafka 2.0 -> 2.4 (Confluent 5.4) - Stream reactor 1.2.7

## DEPRECATION NOTICE

In the next major release, Elasticsearch 6 support will be removed, to be replaced with OpenSearch and Elasticsearch 8 support.

The following connectors have been deprecated and are no longer included in future releases:

* Elasticsearch 6
* Kudu
* Hazelcast
* HBase
* Hive
* Pulsar

### Connectors

**Please take a moment and read the documentation and make sure the software prerequisites are met!!**

| Connector                   | Type   | Description                                                 | Docs                                                                             |
|-----------------------------|--------|-------------------------------------------------------------|----------------------------------------------------------------------------------|
| AWS S3                      | Sink   | Copy data from Kafka to AWS S3.                             | [Docs](https://docs.lenses.io/5.4/connectors/sinks/s3sinkconnector/)             |
| AWS S3                      | Source | Copy data from AWS S3 to Kafka.                             | [Docs](https://docs.lenses.io/5.4/connectors/sources/s3sourceconnector/)         |
| Azure Data Lake (Beta)      | Sink   | Copy data from Kafka to Azure Data Lake                     | [Docs](https://docs.lenses.io/5.4/connectors/sinks/datalakesinkconnector/)       |
| AzureDocumentDb             | Sink   | Copy data from Kafka and Azure Document Db.                 | [Docs](https://docs.lenses.io/connectors/sink/azuredocdb.html)                   |
| Cassandra                   | Source | Copy data from Cassandra to Kafka.                          | [Docs](https://docs.lenses.io/connectors/source/cassandra.html)                  |
| *Cassandra                  | Sink   | Certified DSE Cassandra, copy data from Kafka to Cassandra. | [Docs](https://docs.lenses.io/connectors/sink/cassandra.html)                    |
| Elastic 6                   | Sink   | Copy data from Kafka to Elastic Search 6.x w. tcp or http   | [Docs](https://docs.lenses.io/connectors/sink/elastic6.html)                     |
| Elastic 7                   | Sink   | Copy data from Kafka to Elastic Search 7.x w. tcp or http   | [Docs](https://docs.lenses.io/connectors/sink/elastic7.html)                     |
| FTP/HTTP                    | Source | Copy data from FTP/HTTP to Kafka.                           | [Docs](https://docs.lenses.io/5.4/connectors/sources/ftpsourceconnector/)        |
| Google Cloud Storage (Beta) | Sink   | Copy data from Kafka to Google Cloud Storage.               | [Docs](https://docs.lenses.io/5.4/connectors/sinks/gcpstoragesinkconnector/)     |
| Google Cloud Storage (Beta) | Source | Copy data from Google Cloud Storage to Kafka.               | [Docs](https://docs.lenses.io/5.4/connectors/sources/gcpstoragesourceconnector/) |
| HTTP (Beta)                 | Sink   | Copy data from Kafka to HTTP.                               | [Docs](https://docs.lenses.io/5.4/connectors/sinks/httpsinkconnector/)           | 
| InfluxDb                    | Sink   | Copy data from Kafka to InfluxDb.                           | [Docs](https://docs.lenses.io/5.4/connectors/sinks/influxsinkconnector/)         |
| JMS                         | Source | Copy data from JMS topics/queues to Kafka.                  | [Docs](https://docs.lenses.io/connectors/source/jms.html)                        |
| JMS                         | Sink   | Copy data from Kafka to JMS.                                | [Docs](https://docs.lenses.io/connectors/sink/jms.html)                          |
| MongoDB                     | Sink   | Copy data from Kafka to MongoDB.                            | [Docs](https://docs.lenses.io/connectors/sink/mongo.html)                        |
| MQTT                        | Source | Copy data from MQTT to Kafka.                               | [Docs](https://docs.lenses.io/connectors/source/mqtt.html)                       |
| MQTT                        | Sink   | Copy data from Kafka to MQTT.                               | [Docs](https://docs.lenses.io/connectors/sink/mqtt.html)                         |
| Redis                       | Sink   | Copy data from Kafka to Redis.                              | [Docs](https://docs.lenses.io/connectors/sink/redis.html)                        |

## Release Notes

Please see the
*[Stream Reactor Release Notes at Lenses Documentation](https://docs.lenses.io/5.4/connectors/release-notes/)*.

### Building

To build:

```bash
sbt clean compile
```

To test:

```bash
sbt test
```

To create assemblies:

```bash
sbt assembly
```

To build a particular project:

```bash
sbt "project cassandra" compile
```

To test a particular project:

```bash
sbt "project cassandra" test
```

To create a jar of a particular project:

```bash
sbt "project cassandra" assembly
```

### Running E2E tests

If not already built, you must first build the connector archives:

```bash
sbt "project cassandra" assembly
sbt "project elastic6" assembly 
sbt "project mongodb" assembly
sbt "project redis" assembly
```

To run the tests:

```bash
sbt e2e:test
```

### Github Workflows

For a detailed explanation of the Github workflow, please [see our Github Actions Workflow Guide](WORKFLOW.md).

## Contributing

We'd love to accept your contributions! Please use GitHub pull requests: fork the repo, develop and test your code,
[semantically commit](http://karma-runner.github.io/1.0/dev/git-commit-msg.html) and submit a pull request. Thanks!

## License

[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Flensesio%2Fstream-reactor.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Flensesio%2Fstream-reactor?ref=badge_large)

