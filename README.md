# kafka-connect-bigquery

This is an implementation of a sink connector from [Apache Kafka](http://kafka.apache.org/) to
[Google BigQuery](https://cloud.google.com/bigquery/), built on top of
[Apache Kafka Connect](http://docs.confluent.io/3.0.0/connect/).

## Standalone Quickstart

**NOTE**: You must have the [Confluent Platform](http://docs.confluent.io/3.0.0/installation.html)
installed in order to run the example.

### Configuration Basics

First of all, you need to specify a few configuration settings for your connector. These can be
found in the quickstart/properties/connector.properties file. Look for this section:

```
########################################### Fill me in! ###########################################
# The name of the BigQuery project to write to
project=
# The name of the BigQuery dataset to write to (leave the '.*=' at the beginning, enter your
# dataset after it)
datasets=.*=
# The location of a BigQuery service account JSON key file
keyfile=
```

You'll need to pick a BigQuery project to write to, a dataset to write to in that project, and
provide the location of a JSON key file that can be used to access a BigQuery service account that
can write to the project/dataset pair. Once you've decided on these properties, fill them in and
save the properties file.

Once you get more familiar with the connector, you might want to revisit the connector.properties
file and experiment with tweaking its settings.

### Building and extracting a Tarball

Begin by creating a tarball of the connector:

`$ ./gradlew tarBall`

And then extract its contents:

`$ tar -C build/distributions/ -xvf build/distributions/kafka-connect-bigquery-dist-0.2.tar`

### Setting up background processes

Then move into the quickstart directory:

`$ cd quickstart`

After that, if your confluent installation isn't in a sibling directory to
the connector, specify its location (and do so before starting each of the
subsequent processes in their own terminal):

`$ export CONFLUENT_DIR=/path/to/confluent`

Then, initialize the background processes necessary for Kafka Connect (one terminal per script):
(Taken from http://docs.confluent.io/3.0.0/quickstart.html)

`$ ./zookeeper.sh`

(wait a little while for it to get on its feet)

`$ ./kafka.sh`

(wait a little while for it to get on its feet)

`$ ./schema-registry.sh`

(wait a little while for it to get on its feet)

### Initializing the Avro Console Producer

Next, initialize the avro console producer (also in its own terminal):

`$ ./avro-console-producer.sh`

Give it some data to start off with (type directly into the avro-console-producer instance):

`{"f1":"Testing the Kafka-BigQuery Connector!"}`

### Running the Connector

Finally, initialize the BigQuery connector (also in its own terminal):

`$ ./connector.sh`

### Piping data Through the Connector

Now you can enter Avro messages of the schema `{"f1":"$SOME_STRING"}` into
the avro console producer instance, and the pipeline instance should write
them to BigQuery.

If/when you want to get more adventurous, you can experiment with different
schemas and/or topics by adjusting flags given to the avro-console-producer
and tweaking the config settings found in the quickstart/properties directory.

## Integration Testing

**NOTE**: You must have Docker installed and running on your machine in order to run integration
tests for the connector.

### How Integration Testing Works

Integration tests run by creating Docker instances for Zookeeper, Kafka, Schema Registry, and the
Kafka-BigQuery connector itself, then verifying the results using a Junit test.

They use schemas and data that can be found in the test/docker/populate/test_schemas/ directory, and
rely on a user-provided JSON key file (like in the quickstart example) to access BigQuery.

The project and dataset they write to, as well as the specific JSON key file they use, can be
specified by command-line flag, environment variable, or configuration file--the exact details of
each can be found by running the integration test script with the '-?' flag.

### Data Corruption Concerns

In order to ensure the validity of each test, any table that will be written to in the course of
integration testing is preemptively deleted before the connector is run. This will only be an issue
if you have any tables in your dataset whose names begin with 'kcbq_test_' and match the sanitized
name of any of the test_schema subdirectories. If that is the case, you should probably consider
writing to a different project/dataset.

Because Kafka and Schema Registry are run in Docker, there is no risk that running integration tests
will corrupt any existing Kafka/Schema Registry data that is already on your machine, and there is
also no need to free up any of your ports that might currently be in use by real instances of the
programs that are faked in the process of testing.

### Running the Integration Tests

Running the series of integration tests is easy:

`$ test/integrationtest.sh` (assuming project, dataset, and key file have been specified by variable
and/or configuration file--for more information on how to specify these, run the test script with
the `--usage` flag)

NOTE: You must have a recent version of boot2docker, docker-machine, docker, etc installed. Older
versions will hang when cleaning containers, and linking doesn't work properly.

### Adding new Integration Tests

Adding an integration test is a little more involved, and consists of two major steps: specifying
Avro data to be sent to Kafka, and specifying via Junit test how to verify that such data made it
to BigQuery as expected.

To specify input data, you must create a new directory in the test/resources/test_schemas/
directory with whatever name you want the Kafka topic of your test to be named, and whatever string
you want the name of your test's BigQuery table to be derived from. Then, create two files in that
directory: *schema.json* will contain the Avro schema of the type of data the new test will send
through the connector, and *data.json* will contain a series of JSON objects, each of which should
represent an Avro record that matches the specified schema. **Each JSON object must occupy its own
line, and each object cannot occupy more than one line** (this inconvenience is due to limitations
in the Kafka Avro console producer, and may be addressed in future commits).

To specify data verification, add a new Junit test to the file
src/integration-test/java/com/wepay/kafka/connect/bigquery/it/BigQueryConnectorIntegrationTest.java.
Rows that are retrieved from BigQuery in the test are only returned as lists of Objects--the names
of their columns are not tracked. Construct a List of the Objects that you expect to be stored in
the test's BigQuery table, retrieve the actual List of Objects stored via a call to readAllRows(),
and then compare the two via a call to testRows(). **NOTE: Because the order of rows is not
guaranteed when reading test results from BigQuery, you must include a row number as the first field
of any of your test schemas, and every row of test data must have a unique value for its row number
(row numbers are one-indexed).**
