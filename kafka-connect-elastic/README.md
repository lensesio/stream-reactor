![](../images/DM-logo.jpg)
[![Documentation Status](http://docs.datamountaineer.com/en/latest/badge/?version=latest)](http://docs.datamountaineer.com/en/latest/elastic.html)

# Kafka Connect Elastic

A Connector and Sink to write events from Kafka to Elastic Search using [Elastic4s](https://github.com/sksamuel/elastic4s) client. The connector converts the value from the Kafka Connect SinkRecords to Json and uses Elastic4s's JSON insert functionality to index.

The Sink creates an Index and Type corresponding to the topic name and uses the JSON insert functionality from Elastic4s

## Prerequisites
* Confluent 2.0.1
* Elastic Search 2.2
* Java 1.8 
* Scala 2.11

##Build

````bash
 gradle compile
 ```
 
 To test
 
 ```bash
 gradle test
 ```
 
 To create a fat jar
 
 ```bash
 gradle fatJar
 ```
 
 or with no tests run
 
 ```
 gradle fatJarNoTest
 ```
 
 You can also use the gradle wrapper
 
 ```
 ./gradlew fatJar
 ```
 
See the documentation for more information.