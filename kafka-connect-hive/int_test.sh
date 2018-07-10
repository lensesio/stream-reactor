#!/bin/bash

docker-compose -d -f $COMPONENT/docker-compose.yml up

cat << EOF > hive_sink.json
    {
      "name": "hive_sink_test",
      "config": {
        "connector.class": "com.landoop.streamreactor.connect.hive.sink.HiveSinkConnector"
        "topics": "hive_sink_topic",
        "name": "hive_sink_test",
        "kcql" : "insert into",
        "connect.hive.kcql" : "insert into hive_sink_table select * from hive_sink_topic",
        "connect.hive.fs.defaultFS" : "hdfs://namenode:8020",
        "connect.hive.hive.metastore" : "thrift",
        "connect.hive.hive.metastore.uris" : "thrift://hive-metastore:9083",
        "connect.hive.database.name" : "default"
    }
    EOF

curl -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d hive_sink.json localhost:8083/connectors