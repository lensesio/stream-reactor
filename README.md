![CI](https://github.com/lensesio/kafka-connect-query-language/workflows/CI/badge.svg)
[<img src="https://img.shields.io/badge/latest%20release-v2.9.0-blue.svg?label=latest%20release"/>](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.datamountaineer%22%20AND%20a%3A%22kcql%22)


# Kafka Connect Query Language

The **KCQL** (**K**afka **C**onnect **Q**uery **L**anguages) is a SQL like syntax allowing a streamlined configuration of a Kafka Connect Sink/Source. It is build using the <a href="https://github.com/antlr/grammars-v4">`antlr4`</a> API.

# Why ?

While working on our sink/sources we ended up producing quite complex configuration in order to support the functionality required. Imagine a sink where you source from different topics and from each topic you want to cherry pick the payload fields or even rename them. Furthermore, you might want the storage structure to be automatically created and/or even evolve or you might add new support for the likes of bucketing (Riak TS has one such scenario). Imagine the JDBC sink with a table which needs to be linked to two different topics and the fields in there need to be aligned with the table column names and the complex configuration involved ...or you can just write this

```bash
routes.query = "INSERT INTO transactions SELECT field1 as column1, field2 as column2, field3 FROM topic_A;
                INSERT INTO transactions SELECT fieldA1 as column1, fieldA2 as column2, fieldC FROM topic_B;"
```

# Compile and Build
This project is using the Gradle build system. So to build you would simply do
```bash
gradle clean build
```
If you modify the grammar you would need to first compile before the changes are reflected in the code. The antlr gradle plugin would run first and produced the java classes
for the parser and lexer.

# Using KCQL in your project 
To include it in your project you, include it in your connector.

Maven
```bash
<dependency>
	<groupId>com.datamountaineer</groupId>
	<artifactId>kcql</artifactId>
	<version>LATEST</version>
</dependency>
```

Check <a href="http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22kcql%22">Maven</a> for latest release.


# Kafka Connect Query Language 

There are three modes in the DSL : INSERT, UPSERT and SELECT.

The *INSERT* and *UPSERT* queries, support some of the following configurations:

```bash
INSERT INTO $TARGET 
SELECT *|columns 
FROM   $TOPIC_NAME 
       [ IGNORE columns ]
       [ AUTOCREATE ]
       [ PK columns ]
       [ AUTOEVOLVE ]
       [ BATCH = N ]
       [ CAPITALIZE ]
       [ INITIALIZE ]
       [ PARTITIONBY cola[,colb] ]
       [ DISTRIBUTEBY cola[,colb] ]
       [ CLUSTERBY cola[,colb] ]
       [ WITHTIMESTAMP cola|sys_time() ]
       [ STOREAS $YOUR_TYPE([key=value, .....]) ]
       [ WITHFORMAT TEXT|AVRO|JSON|BINARY|OBJECT|MAP ]
```

* To view how a sink connector (i.e. Cassandra) manage configuration options, refer to
<a href="http://docs.datamountaineer.com/en/latest/connectors.html">documentation here</a>

The *SELECT* mode is usefull for target systems that do not support the concept of <namespace> (i.e. a an in-memory
Key-Value system does not have the concept of a <database>.<table>) and can also be utilized in the socket-streamer
to peek into KAFKA via websockets and receive the payloads in real time.

```bash
SELECT *|columns 
FROM   $TOPIC_NAME 
       [ IGNORE columns ]
       [ WITHFORMAT  JSON|AVRO|BINARY ]
       [ WITHGROUP $YOUR_CONSUMER_GROUP ]
       [ WITHPARTITION (partition),[(partition, offset) ]
       [ STOREAS $YOUR_TYPE([key=value, .....]) ]
       [ SAMPLE $RECORDS_NUMBER EVERY $SLIDE_WINDOW ]
```

### Examples of SELECT

    SELECT field1 FROM mytopic                    // Project one avro field named field1
    SELECT field1 AS newName                      // Project and renames a field
    SELECT * FROM mytopic                         // Select everything - perfect for avro evolution
    SELECT *, field1 AS newName FROM mytopic      // Select all & rename a field - excellent for avro evolution
    SELECT * FROM mytopic IGNORE badField         // Select all & ignore a field - excellent for avro evolution
    SELECT * FROM mytopic PK field1,field2        // Select all & with primary keys (for the sources where primary keys are required)
    SELECT * FROM mytopic AUTOCREATE              // Select all and create the target source (table for databases)
    SELECT * FROM mytopic AUTOEVOLVE              // Select all & reflect the new fields added to the avro payload into the target

### Future options

    .. NOOP | THROW | RETRY                          // Define the error policy 
    .. WHERE ..                                      // Add filtering rules
  
