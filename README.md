# kafka-connector-query-language

The Kafka **connector query language** is implemented in `antlr4` grammar files.

You can find example grammars <a href="https://github.com/antlr/grammars-v4">here</a>

# Why ?

A Kafka Connect **CQL** (**C**onnector **Q**uery **L**anguages) makes a lot of sense when you need to define mappings between 
Kafka topics with Avro records and external systems as _sinks_ or _sources_. 

# Connector Query Language 

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
    
## Building

Get this repository and run:

    mvn clean compile test

Java files are generate under `generated-sources/antlr4` folder