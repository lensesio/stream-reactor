Kafka Connect JMS
===================

A Connector and Sink to write events from Kafka to JMS. The connector takes the value from the Kafka Connect SinkRecords
and pushes them to a JMS topic/queuea.

Prerequisites
-------------

- Confluent 2.0
- Any JMS framework
- Java 1.8
- Scala 2.11

Setup
-----

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

In ``/etc/kafka/server.properties`` add the following so we can delete topics.

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

Sink Connector Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Next we start the connector in standalone mode. This useful for testing and one of jobs, usually you'd run in distributed
mode to get fault tolerance and better performance.

Before we can start the connector we need to setup it's configuration. In standalone mode this is done by creating a
properties file and passing this to the connector at startup. In distributed mode you can post in the configuration as
json to the Connectors HTTP endpoint. Each connector exposes a rest endpoint for stopping, starting and updating the
configuration.

Since we are in standalone mode we'll create a file called ``hbase-sink.properties`` with the contents below:

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
2.  The sink class name

Starting the Sink Connector (Standalone)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Now we are ready to start the JMS sink Connector in standalone mode.

.. note::

    You need to add the connector to your classpath or you can create a folder in ``share/java`` of the Confluent
    install location like, kafka-connect-myconnector and the start scripts provided by Confluent will pick it up.
    The start script looks for folders beginning with kafka-connect.

.. code:: bash

    #Add the Connector to the class path
    ➜  export CLASSPATH=kafka-connect-jms-0.1-all.jar
    #Start the connector in standalone mode, passing in two properties files, the first for the schema registry, kafka
    #and zookeeper and the second with the connector properties.
    ➜  bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties jms-sink.properties


Test Records
^^^^^^^^^^^^

Now we need to put some records it to the test_table topics. We can use the ``kafka-avro-console-producer`` to do this.

Start the producer and pass in a schema to register in the Schema Registry. The schema has a ``firstname`` field of type string
a ``lastnamme`` field of type string, an ``age`` field of type int and a ``salary`` field of type double.

.. code:: bash

    bin/kafka-avro-console-producer \
      --broker-list localhost:9092 --topic person_hbase \
      --property value.schema='{"type":"record","name":"User","namespace":"com.datamountaineer.streamreactor.connect.redis", \
      "fields":[{"name":"firstName","type":"string"},{"name":"lastName","type":"string"},{"name":"age","type":"int"}, \
      {"name":"salary","type":"double"}]}'

Now the producer is waiting for input. Paste in the following:

.. code:: bash

    {"firstName": "John", "lastName": "Smith", "age":30, "salary": 4830}
    {"firstName": "Anna", "lastName": "Jones", "age":28, "salary": 5430}

Check for records in your JMS system
~~~~~~~~~~~~~~~~~~~~~~~~~~

Now check the logs of the connector you should see this

.. code:: bash

    INFO Sink task org.apache.kafka.connect.runtime.WorkerSinkTask@48ffb4dc finished initialization and start (org.apache.kafka.connect.runtime.WorkerSinkTask:155)
    INFO Writing 2 records to JMS... (com.datamountaineer.streamreactor.connect.jms.sink.writer.JMSWriter)


Now stop the connector.

Starting the Connector (Distributed)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Connectors can be deployed distributed mode. In this mode one or many connectors are started on the same or different
hosts with the same cluster id. The cluster id can be found in ``etc/schema-registry/connect-avro-distributed.properties.``

.. code:: bash

    # The group ID is a unique identifier for the set of workers that form a single Kafka Connect
    # cluster
    group.id=connect-cluster

For this quick-start we will just use one host.

Now start the connector in distributed mode, this time we only give it one properties file for the kafka, zookeeper and
schema registry configurations.

.. code:: bash

    ➜  confluent-2.0.1/bin/connect-distributed confluent-2.0.1/etc/schema-registry/connect-avro-distributed.properties

Once the connector has started lets use the kafka-connect-tools cli to post in our distributed properties file.

.. code:: bash

    ➜  java -jar build/libs/kafka-connect-cli-0.2-all.jar create jms-sink < jms-sink.properties

If you switch back to the terminal you started the Connector in you
should see the HBase sink being accepted and the task starting.


Features
--------

The JMS sink writes records from Kafka to JMS topics/queues.

The sink supports:

1. Message Type - specifies which JMS Message type to send over the wire; JSON-will create a TextMessage with the payload the
json for the SinkRecord;AVRO - Creates a BytesMessage with the payload being an array of bytes resulted from the Avro record;
OBJECT - Creates an ObjectMapMessage; MAP- Creates a MapMessage
2. Field selection - Kafka topic payload field selection is supported, allowing you to have choose selection of fields


Configurations
--------------

+----------------------------------+-----------+----------+-----------------------------------+
| name                             | data type | required | description                       |
+==================================+===========+==========+===================================+
|

Example
~~~~~~~

.. code:: bash


    connector.class=com.datamountaineer.streamreactor.connect.jms.sink.JMSSinkConnector
    tasks.max=1
    topics=person_activemq
    name=person-jms-test

Schema Evolution
----------------

Upstream changes to schemas are handled by Schema registry which will validate the addition and removal
or fields, data type changes and if defaults are set. The Schema Registry enforces Avro schema evolution rules.
More information can be found `here <http://docs.confluent.io/2.0.1/schema-registry/docs/api.html#compatibility>`_.


Deployment Guidelines
---------------------

TODO

TroubleShooting
---------------

TODO
