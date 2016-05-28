# kafka-connector-query-language

The Kafka **connector query language** is implemented in `antlr4` grammar files.
Available at src/main/antlr/com/datamountaineer/connector/querylanguage

You can find example grammars <a href="https://github.com/antlr/grammars-v4">here</a>

# Why ?

A **C**onnector **q**uery **l**anguages (Cql) makes a lot of sense when you need to define mappings between 
Kafka topics and external systems as _sinks_ or _sources_. 

# Connector Query Language 

    INSERT into TARGET_SQL_TABLE SELECT * FROM SOURCE_TOPIC IGNORE a,b,c

    SELECT field1                                 // Project one avro field named field1
    SELECT field1.subfield1                       // Project one avro field from a complex message
    SELECT field1 AS newName                      // Project and renames a field
    SELECT *                                      // Select everything - perfect for avro evolution
    SELECT * RENAME field1 AS newName             // Rename a field - excellent for avro evolution
    SELECT * IGNORE badField                      // Ignore a field - excellent for avro evolution

    UPSERT into TARGET_SQL_TABLE SELECT ...       // INSERT & UPSERT allowed
    UPSERT PK field1 into TARGET_SQL_TABLE ..     // UPSERT on particular Primary Keys

    ... AUTOCREATE                                // AUTOCREATE TABLE
    ... AUTOCREATE PK field1,field2               // AUTOCREATE with Primary Keys
    ... AUTOEVOLVE
    ... AUTOCREATE AUTOEVOLVE
    ... BATCH 5000                                // SET BATCHING TO 5000 records
    ... NOOP 
    ... THROW
    ... RETRY
    
## Building

Get this repository and run:

    mvn clean compile test

Java files are generate under `generated-sources/antlr4` folder

## Testing

Install the antlr4test maven plugin

    mkdir temp; cd temp
    git clone git@github.com:antlr/grammars-v4.git
    cd grammars-v4/support/antlr4test-maven-plugin
    mvn clean install 
    cd ../../../..

And then 
