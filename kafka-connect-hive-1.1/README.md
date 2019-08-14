## Hive Connector Quick Start

There is a docker container for Apache Hive 2.3.2 in the folder `it`.

It is based on https://github.com/big-data-europe/docker-hadoop so check there for Hadoop configurations.
It also contains **Lenses fast-data-dev** (https://github.com/Landoop/fast-data-dev)

It deploys Hive and starts a hiveserver2 on port 10000.
Metastore is running with a connection to postgresql database.
The hive configuration is performed with HIVE_SITE_CONF_ variables (see hadoop-hive.env for an example).

To run Hive with postgresql metastore:
```
    docker-compose up -d
```
To stop Hive:
```
    docker-compose down --volumes
```
## Connector fat jar and configuration

The kafka-connect-hive fat jar must be placed inside the path ``build/libs/`` in order the corresponding volume to be created accordingly.

There is a kafka-connect-hive configuration example in the ``stream-reactor/conf/`` folder.

## Testing

You can start the Hive shell, which uses Beeline in the background, to enter HiveQL commands on the command line of a node in a cluster.

Start a Hive shell locally:
```
  $ docker-compose exec hive-server bash
  # /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000
  > 
```

Load data into Hive:
```
  $ docker-compose exec hive-server bash
  # /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000
  > create database hive_connect;
  > use hive_connect;
  > create table cities (city string, state string, population int, country string) stored as parquet;
  > insert into table cities values ("Philadelphia", "PA", 1568000, "USA");
  > insert into table cities values ("Chicago", "IL", 2705000, "USA");
  > insert into table cities values ("New York", "NY", 8538000, "USA");
  > 
```

You can specify multiple statements separated by ``;``

Enter a HiveQL query:
```
  > select * from cities;
  New York NY 8538000 USA
  Chicago IL 2705000 USA
  Philadelphia PA 1568000 USA

  Time taken: 0.12 seconds, Fetched: 3 row(s)
```
