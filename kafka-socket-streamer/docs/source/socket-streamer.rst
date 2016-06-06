Kafka Socket Streamer
=====================

Akka Http with Reactive Kafka to stream topics to clients via Web sockets and Server Send Events.

**This is test and not yet intended for any serious use yet.**

Prerequisites
-------------


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

QuickStart
--------------

The socket streamer pushes events out from Kafka to clients via websockets or server send events. Two different endpoints
are available. But first we need some data in Kafka. Start the console producer and send some event in:

.. code:: bash

    ➜   bin/kafka-avro-console-producer \
      --broker-list localhost:9092 --topic socket_streamer \
      --property value.schema='{"type":"record","name":"User","namespace":"com.datamountaineer.streamreactor.connect.redis"
      ,"fields":[{"name":"firstName","type":"string"},{"name":"lastName","type":"string"},{"name":"age","type":"int"},
      {"name":"salary","type":"double"}]}'


Now paste in the following:

.. code:: bash

{"firstName": "John", "lastName": "Smith", "age":30, "salary": 4830}
{"firstName": "Max", "lastName": "Power", "age":30, "salary": 1000000}


Now start the socket streamer. We need to set some configurations first. The socket-streamer uses Typesafe's configuration
loader so we can create a file called ``application.conf`` and add the following.

.. code:: bash

    system-name = "streamreactor-socket-streamer"
    port = 8080

    kafka {
      bootstrap-servers = "localhost:9092"
      zookeeper-servers = "localhost:2181"
      schema-registry-url = "http://localhost:8081"
    }

To start the socket streamer:

.. code:: bash

    ➜   java -jar build/libs/kafka-socket-streamer-0.1-all.jar

    2016-05-12 15:57:39,712 INFO  [main] [c.d.s.s.Main$] [delayedEndpoint$com$datamountaineer$streamreactor$socketstreamer$Main$1:32]

        ____        __        __  ___                  __        _
/ __ \____ _/ /_____ _/  |/  /___  __  ______  / /_____ _(_)___  ___  ___  _____
/ / / / __ `/ __/ __ `/ /|_/ / __ \/ / / / __ \/ __/ __ `/ / __ \/ _ \/ _ \/ ___/
     / /_/ / /_/ / /_/ /_/ / /  / / /_/ / /_/ / / / / /_/ /_/ / / / / /  __/  __/ /
/_____/\__,_/\__/\__,_/_/  /_/\____/\__,_/_/ /_/\__/\__,_/_/_/ /_/\___/\___/_/
      _____            __        __  _____ __
/ ___/____  _____/ /_____  / /_/ ___// /_________  ____ _____ ___  ___  _____
\__ \/ __ \/ ___/ //_/ _ \/ __/\__ \/ __/ ___/ _ \/ __ `/ __ `__ \/ _ \/ ___/
     ___/ / /_/ / /__/ ,< /  __/ /_ ___/ / /_/ /  /  __/ /_/ / / / / / /  __/ /
/____/\____/\___/_/|_|\___/\__//____/\__/_/   \___/\__,_/_/ /_/ /_/\___/_/

    by Andrew Stevenson

    2016-05-12 15:57:39,716 INFO  [main] [c.d.s.s.Main$] [delayedEndpoint$com$datamountaineer$streamreactor$socketstreamer$Main$1:49]
    System name      : streamreactor-socket-streamer
    Kafka brokers    : localhost:9092
    Zookeepers       : localhost:2181
    Schema registry  : http://localhost:8081
    Listening on port : 8080



Now lets have the socket streamer push use server send event by simply calling curl:

.. code:: bash

    ➜  curl 'http://localhost:8080/sse/topics?topic=socket_streamer&consumergroup=testcg'

    data:{"value":"{\"firstName\": \"John\", \"lastName\": \"Smith\", \"age\": 30, \"salary\": 4830.0}"}
    data:{"value":"{\"firstName\": \"Max\", \"Power\": \"Jones\", \"age\": 30, \"salary\": 1000000}"}
    data:{"timestamp":"Thu May 12 16:42:02 CEST 2016","system":"streamreactor-socket-streamer","message":"heartbeat"}

For websockets, install a websocket client, for example `Dark WebSocket Terminal <http://tinyurl.com/nqc9s3c>`_. Start
it and connect to the websocket endpoint.

.. note:: Dark Terminal, for some reason, needs a extra whitespace at the end of the connection url to work.

.. code:: bash

    command:	/connect ws://localhost:8080/ws/topics?topic=person_redis&consumergroup=testcgws
    system:	connection established, ws://localhost:8080/ws/topics?topic=person_redis&consumergroup=testcgws
    received:	{"value":"{\"firstName\": \"John\", \"lastName\": \"Smith\", \"age\": 30, \"salary\": 4830.0}"}


Features
--------

1. Web Sockets
2. Server Send Events
3. HeartBeat Messages

Configurations
--------------

Endpoints
---------

Example
~~~~~~~

... code:: bash


Deployment Guidelines
---------------------

TODO

TroubleShooting
---------------

TODO
