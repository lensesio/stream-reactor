.. kafka-connect-hbase:

Kafka Connect HBase
===================

A Connector and Sink to write events from Kafka to HBase. The connector takes the value from the Kafka Connect SinkRecords and inserts a new entry to HBase.

Prerequisites
-------------

- Confluent 2.0
- HBase 1.2.0
- Java 1.8
- Scala 2.11

Setup
-----

HBase Setup
~~~~~~~~~~~

Confluent Setup
~~~~~~~~~~~~~~~

.. code:: bash

    #make confluent home folder
    mkdir confluent

    #download confluent
    wget http://packages.confluent.io/archive/2.0/confluent-2.0.1-2.11.7.tar.gz

    #extract archive to confluent folder
    tar -xvf confluent-2.0.1-2.11.7.tar.gz -C confluent

    #setup variables
    export CONFLUENT_HOME=~/confluent/confluent-2.0.1

Enable topic deletion.

In ``/etc/kafka/server.properties`` add the following to we can delete
topics.

.. code:: bash

    delete.topic.enable=true

Start the Confluent platform.

.. code:: bash

    #Start the confluent platform, we need kafka, zookeeper and the schema registry
    bin/zookeeper-server-start etc/kafka/zookeeper.properties &
    bin/kafka-server-start etc/kafka/server.properties &
    bin/schema-registry-start etc/schema-registry/schema-registry.properties &

Build the Connector and CLI
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The prebuilt jars can be taken from here and
`here <https://github.com/datamountaineer/kafka-connect-tools/releases>`__
or from `Maven <http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22kafka-connect-cli%22>`__

If you want to build the connector, clone the repo and build the jar.

.. code:: bash

    ##Build the connectors
    git clone https://github.com/datamountaineer/stream-reactor
    cd stream-reactor
    gradle fatJar

    ##Build the CLI for interacting with Kafka connectors
    git clone https://github.com/datamountaineer/kafka-connect-tools
    cd kafka-connect-tools
    gradle fatJar

Sink Connector QuickStart
-------------------------

HBase Table
~~~~~~~~~~~

The sink expects a precreated table in HBase. In the HBase shell create the test table:

.. code:: bash

    create 'person',{NAME=>'d', VERSIONS=>1}

Sink Connector Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Next we start the connector in standalone mode. This useful for testing
and one of jobs, usually you'd run in distributed mode to get fault
tolerance and better performance.

Before we can start the connector we need to setup it's configuration.
In standalone mode this is done by creating a properties file and
passing this to the connector at startup. In distributed mode you can
post in the configuration as json to the Connectors HTTP endpoint. Each
connector exposes a rest endpoint for stopping, starting and updating the
configuration.

Since we are in standalone mode we'll create a file called
hbase-sink.properties with the contents below:

.. code:: bash

    name=person-hbase-test
    connect.hbase.sink.rowkey.mode=FIELDS
    connect.hbase.sink.fields=firstName,lastName,age,salary=income
    connector.class=com.datamountaineer.streamreactor.connect.hbase.HbaseSinkConnector
    tasks.max=1
    topics=person_hbase
    connect.hbase.sink.table.name=person_hbase
    connect.hbase.sink.column.family=d
    connect.hbase.sink.key=firstName,lastName

This configuration defines:

1.  The name of the sink.
2.  The key mode. There are three available modes: SINK_RECORD, FIELDS and GENERIC. SINK_RECORD, uses the SinkRecord.keyValue as the hbase row key, FIELDS, combines the specified payload (kafka connect Struct instance) fields to make up the HBase row key ,GENERIC, combines the kafka topic, offset and partition to build the hbase row key.
3.  The fields to extract from the source topics payload.
4.  The sink class.
5.  The max number of tasks the connector is allowed to created. Should not be greater than the number of partitions in the source topics
    otherwise tasks will be idle.
6.  The source kafka topics to take events from.
7.  The HBase table to write to.
8.  The HBase column family to write to.
9.  The topic payload fields to use and the row key in Hbase.

Starting the Sink Connector (Standalone)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Now we are ready to start the hbase sink Connector in standalone mode.

.. note:: You need to add the connector to your classpath or you can create a folder in share/java like kafka-connect-myconnector and the start scripts provided by Confluent will pick it up. The start script looks for folders beginning with kafka-connect.

.. code:: bash

    #Add the Connector to the class path
    ➜  export CLASSPATH=kafka-connect-hbase-0.1-all.jar
    #Start the connector in standalone mode, passing in two properties files, the first for the schema registry, kafka and zookeeper and the second with the connector properties.
    ➜  bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties hbase-sink.properties

We can use the CLI to check if the connector is up but you should be able to see this in logs as-well.

.. code:: bash

    ➜ java -jar build/libs/kafka-connect-cli-0.2-all.jar get hbase-sink


Test Records
^^^^^^^^^^^^

Now we need to put some records it to the test_table topics. We can use the ``kafka-avro-console-producer`` to do this.

Start the producer and pass in a schema to register in the Schema Registry. The schema has a ``firstname`` field of type string
a ``lastnamme`` field of type string, an ``age`` field of type int and a ``salary`` field of type double.

.. code:: bash

    bin/kafka-avro-console-producer \
      --broker-list localhost:9092 --topic person_hbase \
      --property value.schema='{"type":"record","name":"User","namespace":"com.datamountaineer.streamreactor.connect.redis","fields":[{"name":"firstName","type":"string"},{"name":"lastName","type":"string"},{"name":"age","type":"int"},{"name":"salary","type":"double"}]}'

Now the producer is waiting for input. Paste in the following:

.. code:: bash

    {"firstName": "John", "lastName": "Smith", "age":30, "salary": 4830}
    {"firstName": "Anna", "lastName": "Jones", "age":28, "salary": 5430}

Check for records in HBase
~~~~~~~~~~~~~~~~~~~~~~~~~~

Now check the logs of the connector you should see this

... code:: bash

Now stop the connector.

Starting the Connector (Distributed)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Connectors can be deployed distributed mode. In this mode one or many
connectors are started on the same or different hosts with the same cluster id.
The cluster id can be found in ``etc/schema-registry/connect-avro-distributed.properties.``

.. code:: bash

    # The group ID is a unique identifier for the set of workers that form a single Kafka Connect
    # cluster
    group.id=connect-cluster

For this quick-start we will just use one host.

Now start the connector in distributed mode, this time we only give it
one properties file for the kafka, zookeeper and schema registry
configurations.

.. code:: bash

    ➜  confluent-2.0.1/bin/connect-distributed confluent-2.0.1/etc/schema-registry/connect-avro-distributed.properties

Once the connector has started lets use the kafka-connect-tools cli to
post in our distributed properties file.

.. code:: bash

    ➜  java -jar build/libs/kafka-connect-cli-0.2-all.jar create hbase-sink < hbase-sink.properties

If you switch back to the terminal you started the Connector in you
should see the HBase sink being accepted and the task starting.


Features
--------

The HBase sink writes records from Kafka to HBase.

The sink supports:

1. Key modes - Allows for custom or automatic HBase key generation. You can specify fields in the topic payload to
   concatenate to form the key, write this a s string or Avro, or have the sink take the key value from the Kafka message.
2. Field selection - Kafka topic payload field selection is supported, allowing you to have choose selection of fields  or all fields written to hbase.

Configurations
--------------

| connect.hbase.sink.key | String | | If row key mode is set to FIELDS this setting is required. Multiple fields can be specified by separating them via a comma; The fields are combined using a key separator by default is set to <\\n>. |



+----------------------------------+-----------+----------+-----------------------------------+
| name                             | data type | required | description                       |
+==================================+===========+==========+===================================+
| connect.hbase.sink.table.name    | String    | Yes      || Specifies the target HBase table |
|                                  |           |          || to insert into.                  |
+----------------------------------+-----------+----------+-----------------------------------+
| connect.hbase.sink.column.family | String    | Yes      || Specifies the table column family|
|                                  |           |          || to use when inserting the new    |
|                                  |           |          || entry columns.                   |
+----------------------------------+-----------+----------+-----------------------------------+
| connect.hbase.sink.key           | String    | Yes      || If row key mode is set to FIELDS |
|                                  |           |          || this setting is required.        |
|                                  |           |          || Multiple fields can be specified |
|                                  |           |          || by separating them via a comma   |
|                                  |           |          || The fields are combined using a  |
|                                  |           |          || key separator by default is set  |
|                                  |           |          || to <\\n>.                        |
+----------------------------------+-----------+----------+-----------------------------------+
| connect.hbase.sink.table.key.mode| String    | Yes      || There are three available modes: |
|                                  |           |          || SINK_RECORD, FIELDS and GENERIC. |
|                                  |           |          || uses the SinkRecord.keyValue as  |
|                                  |           |          || SINK_RECORD.                     |
|                                  |           |          || the HBase row key; FIELDS -      |
|                                  |           |          || combines the specified payload   |
|                                  |           |          || (kafka connect Struct instance)  |
|                                  |           |          || fields to make up the HBase row  |
|                                  |           |          || key; GENERIC- combines the kafka |
|                                  |           |          || topic, offset and partition to   |
|                                  |           |          || build the HBase row key.         |
+----------------------------------+-----------+----------+-----------------------------------+
| connect.hbase.sink.fields        | String    | Yes      || Specifies which fields to        |
|                                  |           |          || consider when inserting the new  |
|                                  |           |          || HBase entry. If is not set it    |
|                                  |           |          || will take all the fields present |
|                                  |           |          || in the payload. Field mapping is |
|                                  |           |          || supported; this way a payload    |
|                                  |           |          || field can be inserted into a     |
|                                  |           |          || 'mapped' column. If this setting |
|                                  |           |          || is not present it will insert all|
|                                  |           |          || fields.  Examples: * fields to be|
|                                  |           |          || used:field1,field2,field3; -     |
|                                  |           |          || Only! field1,field2 and field3   |
|                                  |           |          || will be inserted ** fields with  |
|                                  |           |          || mapping: field1=alias1,field2,   |
|                                  |           |          || field3=alias3 - Only! field1,    |
|                                  |           |          || field2 and field3 will be        |
|                                  |           |          || inserted fields with             |
|                                  |           |          || mapping:\*,field3=alias.         |
|                                  |           |          || All fields are inserted but      |
|                                  |           |          || field3 will be inserted as alias |
+----------------------------------+-----------+----------+-----------------------------------+

Example
~~~~~~~

.. code:: bash

    connect.hbase.sink.rowkey.mode=FIELDS
    connect.hbase.sink.table.name=person
    connect.hbase.sink.column.family=d
    connect.hbase.sink.key=firstName,lastName
    connect.hbase.sink.fields=firstName,lastName,age,salary=income
    connector.class=com.datamountaineer.streamreactor.connect.hbase.HbaseSinkConnector
    tasks.max=1
    topics=person_hbase
    name=person-hbase-test

Schema Evolution
----------------

TODO

Deployment Guidelines
---------------------

TODO

TroubleShooting
---------------

TODO
