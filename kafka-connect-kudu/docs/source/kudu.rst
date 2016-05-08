.. kafka-connect-Kudu:

Kafka Connect Kudu
===================

A Connector and Sink to write events from Kafka to kudu.

The connector takes the value from the Kafka Connect SinkRecords and inserts a new entry to Kudu.

Prerequisites
-------------

- Confluent 2.0
- Kudu 0.7
- Java 1.8
- Scala 2.11

Setup
-----

Kudu Setup
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

Test data
~~~~~~~~~

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
kudu-sink.properties with the contents below:

.. code:: bash

    name=kudu-sink
    connector.class=com.datamountaineer.streamreactor.connect.kudu.KuduSinkConnector
    tasks.max=1
    connect.kudu.master=quickstart
    topics=kudu_test

This configuration defines:

1.  The name of the sink.
2.  The sink class.
3.  The max number of tasks the connector is allowed to created. Should not be greater than the number of partitions in the source topics
    otherwise tasks will be idle.
4.  The source kafka topics to take events from.

Starting the Sink Connector (Standalone)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Now we are ready to start the Kudu sink Connector in standalone mode.

.. note:: You need to add the connector to your classpath or you can create a folder in share/java like kafka-connect-myconnector and the start scripts provided by Confluent will pick it up. The start script looks for folders beginning with kafka-connect.

.. code:: bash

    #Add the Connector to the class path
    ➜  export CLASSPATH=kafka-connect-Kudu-0.1-all.jar
    #Start the connector in standalone mode, passing in two properties files, the first for the schema registry, kafka and zookeeper and the second with the connector properties.
    ➜  bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties kudu-sink.properties

We can use the CLI to check if the connector is up but you should be able to see this in logs as-well.

.. code:: bash

    ➜ java -jar build/libs/kafka-connect-cli-0.2-all.jar get Kudu-sink

Insert Test Data
~~~~~~~~~~~~~~~~

Check for records in Kudu
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

    ➜  confluent-2.0.1/bin/connect-distributed etc/schema-registry/connect-avro-distributed.properties 

Once the connector has started lets use the kafka-connect-tools cli to
post in our distributed properties file.

.. code:: bash

    ➜  java -jar build/libs/kafka-connect-cli-0.2-all.jar create Kudu-sink < kudu-sink.properties

If you switch back to the terminal you started the Connector in you
should see the Kudu sink being accepted and the task starting.

Check the logs.

Check Kafka.

Features
--------


Configurations
--------------

+---------------------+-----------+----------+----------------------------+
| name                | data type | required | description                |
+=====================+===========+==========+============================+
||connect.kudu.master | String    | Yes      || Specifies the Kudu server.|
+---------------------+-----------+----------+----------------------------+

Example
~~~~~~~

.. code:: bash

    name=kudu-sink
    connector.class=com.datamountaineer.streamreactor.connect.kudu.KuduSinkConnector
    tasks.max=1
    connect.kudu.master=quickstart
    topics=kudu_test

Schema Evolution
----------------

TODO

Deployment Guidelines
---------------------

TODO

TroubleShooting
---------------

TODO
