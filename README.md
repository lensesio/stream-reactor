# Kafka Connect BigQuery Connector

[![Build Status](https://img.shields.io/travis/wepay/kafka-connect-bigquery.svg?style=flat-square)](https://travis-ci.org/wepay/kafka-connect-bigquery)
[![Code Coverage](https://img.shields.io/codecov/c/github/wepay/kafka-connect-bigquery.svg?style=flat-square)](https://codecov.io/gh/wepay/kafka-connect-bigquery)

This is an implementation of a sink connector from [Apache Kafka] to [Google BigQuery], built on top 
of [Apache Kafka Connect]. For a comprehensive list of configuration options, see the [Connector Configuration Wiki].

## History

This connector was [originally developed by WePay](https://github.com/wepay/kafka-connect-bigquery).
In late 2020 the project moved to [Confluent](https://github.com/confluentinc/kafka-connect-bigquery),
with both companies taking on maintenance duties. All new activity such as filing issues and opening
pull requests should now target the [Confluent](https://github.com/confluentinc/kafka-connect-bigquery)
fork of the project.

## Download

The latest releases are available in the GitHub release tab, or via [Confluent Hub](https://www.confluent.io/hub/wepay/kafka-connect-bigquery).

## Standalone Quickstart

> **NOTE**: You must have the [Confluent Platform] installed in order to run the example.

### Configuration Basics

Firstly, you need to specify configuration settings for your connector. These can be found in the 
`kcbq-connector/quickstart/properties/connector.properties` file. Look for this section:

```plain
########################################### Fill me in! ###########################################
# The name of the BigQuery project to write to
project=
# The name of the BigQuery dataset to write to (leave the '.*=' at the beginning, enter your
# dataset after it)
datasets=.*=
# The location of a BigQuery service account or user JSON credentials file
# or service account credentials or user credentials in JSON format (non-escaped JSON blob)
keyfile=
# 'FILE' if keyfile is a credentials file, 'JSON' if it's a credentials JSON
keySource=FILE
```

You'll need to choose a BigQuery project to write to, a dataset from that project to write to, and
provide the location of a JSON key file that can be used to access a BigQuery service account that
can write to the project/dataset pair. Once you've decided on these properties, fill them in and
save the properties file.

Once you get more familiar with the connector, you might want to revisit the `connector.properties`
file and experiment with tweaking its settings.

#### Migrating to 2.x.x
In accordance with the introduction of schema unionization in version 2.0.0, the following changes
to configs have been introduced and should be made when migrating:
1. `autoUpdateSchemas` has been removed
2. `allowNewBigQueryFields` and `allowBigQueryRequiredFieldRelaxation` have been introduced
3. `allowSchemaUnionization` has been introduced

Setting `allowNewBigQueryFields` and `allowBigQueryRequiredFieldRelaxation` to `true` while
setting `allowSchemaUnionization` to false results in the same behavior that setting `autoUpdateSchemas`
to `true` used to.

### Building and Extracting a Confluent Hub archive

If you haven't already, move into the repository's top-level directory:

```bash
$ cd /path/to/kafka-connect-bigquery/
```

Begin by creating Confluent Hub archive of the connector with the Confluent Schema Retriever included:

```bash
$ mvn clean package -DskipTests
```

And then extract its contents:

```bash
$ mkdir -p bin/jar/ && cp kcbq-connector/target/components/packages/wepay-kafka-connect-bigquery-*/wepay-kafka-connect-bigquery-*/lib/*.jar bin/jar/
```

### Setting-Up Background Processes

Then move into the `quickstart` directory:

```bash
$ cd kcbq-connector/quickstart/
```

After that, if your Confluent Platform installation isn't in a sibling directory to the connector, 
specify its location (and do so before starting each of the subsequent processes in their own 
terminal):

```bash
$ export CONFLUENT_DIR=/path/to/confluent
```

Then, initialize the background processes necessary for Kafka Connect (one terminal per script):
(Taken from http://docs.confluent.io/3.0.0/quickstart.html)

```bash
$ ./zookeeper.sh
```

(wait a little while for it to get on its feet)

```bash
$ ./kafka.sh
```

(wait a little while for it to get on its feet)

```bash
$ ./schema-registry.sh
```

(wait a little while for it to get on its feet)

### Initializing the Avro Console Producer

Next, initialize the Avro Console Producer (also in its own terminal):

```bash
$ ./avro-console-producer.sh
```

Give it some data to start off with (type directly into the Avro Console Producer instance):

```json
{"f1":"Testing the Kafka-BigQuery Connector!"}
```

### Running the Connector

Finally, initialize the BigQuery connector (also in its own terminal):

```bash
$ ./connector.sh
```

### Piping Data Through the Connector

Now you can enter Avro messages of the schema `{"f1": "$SOME_STRING"}` into the Avro Console 
Producer instance, and the pipeline instance should write them to BigQuery.

If you want to get more adventurous, you can experiment with different schemas or topics by 
adjusting flags given to the Avro Console Producer and tweaking the config settings found in the 
`kcbq-connector/quickstart/properties` directory.

## Integration Testing the Connector

### Configuring the tests

You must supply the following environment variables in order to run the tests:

- `$KCBQ_TEST_PROJECT`: The name of the BigQuery project to use for the test
- `$KCBQ_TEST_DATASET`: The name of the BigQuery dataset to use for the test
- `$KCBQ_TEST_KEYFILE`: The key file used to authenticate with BigQuery during the test
- `$KCBQ_TEST_BUCKET`: The name of the GCS bucket to use (for testing the GCS batch loading feature)

The `$KCBQ_TEST_FOLDER` variable can be supplied to specify which subfolder of the GCS bucket should
be used when testing the GCS batch loading feature; if not supplied, the top-level folder will be
used.

### Adding new GCP Credentials & BigQuery DataSet
This section is optional in case one wants to use a different GCP project and generate new creds for that
- **Create a GCP Service Account:** Follow instructions from https://cloud.google.com/iam/docs/creating-managing-service-accounts e.g.
```
gcloud iam service-accounts create kcbq-test --description="service account key for bigquery sink integration test" --display-name="kcbq-test"
```
- **Create Service Account Keys:** Follow instructions from https://cloud.google.com/iam/docs/creating-managing-service-account-keys e.g.
```
gcloud iam service-accounts keys create /tmp/creds.json --iam-account=kcbq-test@<GCP_PROJECT_NAME>.iam.gserviceaccount.com
```
- **Give BigQuery & Storage Admin Permissions to Service Account:**  
  - Open https://console.cloud.google.com/iam-admin/iam?project=<GCP_PROJECT_NAME>
  - Click on Add and enter New Principal as created above e.g. `kcbq-test@<GCP_PROJECT_NAME>.iam.gserviceaccount.com`
  - Add following 2 roles from "Select a role" drop down menu:
    - BigQuery -> BigQuery Admin
    - Cloud Storage -> Storage Admin
- **Add a BigQuery DataSet into the Project:**
  - Open https://console.cloud.google.com/bigquery?project=<GCP_PROJECT_NAME>
  - Click on the 3 vertical dots against the project name and click on "Create dataset" and follow the steps there.

### Running the Integration Tests

```bash
# (Re)builds the project and runs the integration tests, skipping unit tests to save a bit of time
mvn clean package integration-test -Dskip.unit.tests=true
```

### How Integration Testing Works

Integration tests run by creating embedded instances for [Zookeeper], [Kafka], [Schema Registry],
and the BigQuery Connector itself, then verifying the results using a [JUnit] test.

They use schemas and data that can be found in the
`kcbq-connector/src/test/resources/integration_test_cases/` directory, and rely on a user-provided
JSON key file (like in the `quickstart` example) to access BigQuery.

### Data Corruption Concerns

In order to ensure the validity of each test, any table that will be written to in the course of
integration testing is preemptively deleted before the connector is run. This will only be an issue
if you have any tables in your dataset whose names begin with `kcbq_test_` and match the sanitized
name of any of the `test_schema` subdirectories. If that is the case, you should probably consider
writing to a different project/dataset.

Kafka, Schema Registry, Zookeeper, and Kafka Connect are all run as temporary embedded instances, so
there is no risk that running integration tests will corrupt any existing data that is already on
your machine, and there is also no need to free up any of your ports that might currently be in use
by instances of the services that are brought up in the process of testing.

### Adding New Integration Tests

Adding an integration test is a little more involved, and consists of two major steps: specifying
Avro data to be sent to Kafka, and specifying via JUnit test how to verify that such data made
it to BigQuery as expected.

To specify input data, you must create a new directory in the
`kcbq-connector/src/test/resources/integration_test_cases/` directory with whatever name you want
the Kafka topic of your test to be named, and whatever string you want the name of your test's
BigQuery table to be derived from. Then, create two files in that directory:

* `schema.json` will contain the Avro schema of the type of data the new test will send
through the connector.

* `data.json` will contain a series of JSON objects, each of which should represent an [Avro] record
that matches the specified schema. **Each JSON object must occupy its own line, and each object
cannot occupy more than one line** (this inconvenience is due to limitations in the Avro
Console Producer, and may be addressed in future commits).

To specify data verification, add to the test cases present in the
`kcbq-connector/src/test/java/com/wepay/kafka/connect/bigquery/integration/BigQuerySinkConnectorIT.java`

> **NOTE**: Because the order of rows is not guaranteed when reading test results from BigQuery, 
you must include a numeric column named "row" number in all of your test schemas, and every row of
test data must have a unique value for its row number. When data is read back from BigQuery to
verify its accuracy, it will be returned in ascending order based on that "row" column.

  [Apache Avro]: https://avro.apache.org
  [Apache Kafka Connect]: http://docs.confluent.io/current/connect/
  [Apache Kafka]: http://kafka.apache.org
  [Apache Maven]: https://maven.apache.org
  [Avro]: https://avro.apache.org
  [BigQuery]: https://cloud.google.com/bigquery/
  [Confluent Platform]: http://docs.confluent.io/current/installation.html
  [Connector Configuration Wiki]: https://github.com/wepay/kafka-connect-bigquery/wiki/Connector-Configuration
  [Google BigQuery]: https://cloud.google.com/bigquery/
  [JUnit]: http://junit.org
  [Kafka Connect]: http://docs.confluent.io/current/connect/
  [Kafka]: http://kafka.apache.org
  [Maven]: https://maven.apache.org
  [Schema Registry]: https://github.com/confluentinc/schema-registry
  [Semantic Versioning]: http://semver.org
  [Zookeeper]: https://zookeeper.apache.org

## Implementation details of different modes
### Upsert/Delete with Legacy InsertAll API
Click [here](https://docs.google.com/document/d/1p8_rLQqR9GIALIruB3-MjqR8EgYdaEw2rlFF1fxRJf0/edit#heading=h.lfiuaruj2s8y) to read the implementation details of upsert/delete mode with Legacy InsertAll API

    
