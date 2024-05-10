![Alt text](images/streamreactor-logo.png)
![Actions Status](https://github.com/lensesio/stream-reactor/actions/workflows/build.yml/badge.svg)
[<img src="https://img.shields.io/badge/docs--orange.svg?"/>](https://docs.lenses.io/connectors/)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Flensesio%2Fstream-reactor.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Flensesio%2Fstream-reactor?ref=badge_shield)

Join us on slack [![Alt text](images/slack.jpeg)](https://launchpass.com/lensesio)

# Lenses Connectors for Apache Kafka

Lenses.io is the leader in offering Apache 2 licensed Kafka Connectors (Stream Reactor) since 2016. 

## Enterprise Support for Kafka Connectors 

Lenses offers the leading Developer Experience solution for engineers building real-time applications on any Apache Kafka ([lenses.io](http://www.lenses.io)). Subscribed customers are entitled to full 24x7 support for selected Kafka Connectors. This includes priority over feature requests and security incident SLAs. Email info@lenses.io for more information. 

## Engage with the Community

Speak to us on our Community Slack channel (Register at https://launchpass.com/lensesio) or ask the Community a question in our [Ask Marios](http://www.lenses.io) forum. 

## Version Support Policy

Under our standard support agreement, we provide assistance for the current major version as well as the preceding major version exclusively. For instance, if version 8.x is the current major release, we offer support for Lenses connectors versions within the 8.x and 7.x series.

Enterprise-level support commences from version 7.0 onwards.

Lenses prioritizes backporting fixes rather than incorporating new features, and this is done on a limited basis across select prior releases. For further clarification, please refer to our maintenance policy.

Should you reach out to our support team regarding issues encountered while using an unsupported version, we will direct you to this section of our policy page and encourage you to upgrade.

## Kafka Connectors Roadmap

A series of next-generation Connectors are in active development. Give us your feedback of which connectors we should be working on or to to get the latest information, send us an email at info@lenses.io

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

## Docs & Release Notes

Please see the
*[Stream Reactor Release Notes at Lenses Documentation](https://docs.lenses.io/)*.

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

