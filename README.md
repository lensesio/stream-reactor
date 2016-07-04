[![Build Status](https://travis-ci.org/datamountaineer/kafka-connect-query-language.svg?branch=master)](https://travis-ci.org/datamountaineer/kafka-connect-query-language)
[<img src="https://img.shields.io/badge/latest%20release-v0.3-blue.svg?label=latest%20release"/>](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.datamountaineer%22%20AND%20a%3A%22kafka-connect-query-language%22)


# Kafka Connect Query Language

The **Kafka Connect Query Language** is implemented in `antlr4` grammar files.

You can find example grammars <a href="https://github.com/antlr/grammars-v4">here</a>

Kafka Connect Common is in Maven, include it in your connector.

# Build
```bash
<dependency>
	<groupId>com.datamountaineer</groupId>
	<artifactId>kcql</artifactId>
	<version>0.3</version>
</dependency>
```

sbt
```bash
libraryDependencies += "com.datamountaineer" % "kcql % "0.3"
```

gradle
```bash
com.datamountaineer:kcql:0.3'
```

# Why ?

A Kafka Connect **KCQL** (**K**afka **C**onnect **Q**uery **L**anguages) makes a lot of sense when you need to define mappings between 
Kafka topics (with Avro records) and external systems as _sinks_ or _sources_. 

# Kafka Connect Query Language 

    INSERT into TARGET_SQL_TABLE SELECT * FROM SOURCE_TOPIC IGNORE a,b,c
    UPSERT into TARGET_SQL_TABLE SELECT ..           // INSERT & UPSERT allowed. Works out PK from DB

### Examples of SELECT

    .. SELECT field1                                 // Project one avro field named field1
    .. SELECT field1.subfield1                       // Project one avro field from a complex message
    .. SELECT field1 AS newName                      // Project and renames a field
    .. SELECT *                                      // Select everything - perfect for avro evolution
    .. SELECT *, field1 AS newName                   // Select all & rename a field - excellent for avro evolution
    .. SELECT * IGNORE badField                      // Select all & ignore a field - excellent for avro evolution

### Other operators

    .. AUTOCREATE                                    // AUTOCREATE TABLE
    .. AUTOCREATE PK field1,field2                   // AUTOCREATE with Primary Keys
    .. BATCH 5000                                    // SET BATCHING TO 5000 records
    .. AUTOEVOLVE
    .. AUTOCREATE AUTOEVOLVE

### Future options

    .. NOOP | THROW | RETRY                          // Define the error policy 
    .. WHERE ..                                      // Add filtering rules
    .. CAPITALIZE | TOLOWER                          // Forces TABLE names and COLUMN names to CAPITAL or _lowercase_
    
## Building

Get this repository and run:

    gradle clean compile test

Java files are generate under `generated-sources/antlr4` folder
