# RabbitMQ Connector

Information regarding RabbitMQ can be found [here](https://www.rabbitmq.com/getstarted.html)

## Source Connector

#### Overview

The source connector reads directly from a queue in a RabbitMQ Server. 

If the queue provided in the KCQL exists in the server it will be used with its already existing configuration. 
If not it will be created with the following configuration
```
DURABLE = true
EXCLUSIVE = false
AUTO_DELETE = false
```

In order to receive data in the RabbitMQ queue it is recommended to bind it in an exchange and route data through it.
This can either be done through the RabbitMQ UI or programmatically from your RabbitMQ Client.

#### KCQL Support
```
INSERT INTO kafka_topic SELECT * FROM rabitmq_queue WITHCONVERTER=`myclass`
```

Supported converters are the BytesConverter,JsonSimpleConverter and AvroConverter described [here](https://docs.lenses.io/connectors/source/jms.html)

## Sink Connector

#### Overview

The sink connector will read data from a kafka topic and pass them to a RabbitMQ exchange. 

If the exchange provided in the KCQL exists in the server it will be used with its already existing configuration. 
If the exchange provided does not exist a new one will be created with following configuration
```
DURABLE = true
AUTO_DELETE = false
INTERNAL = false
TYPE=as provided in the WITHTYPE attribute of KCQL
```

All the messages sent to the exchange will use the routing key provided in the WITHTAG attribute of Kcql.

Worker configurations for JSON and AVRO are described [here](https://docs.lenses.io/connectors/source/jms.html)

#### KCQL Support
```
INSERT INTO exchange SELECT * FROM kafka_topic WITHTAG (routing.key) WITHTYPE {fanout,direct,topic}
```

## Configurations

#### Common for source and sink

|Config| Description | Type |Default|
|------|---------|---------|--|
|name|Name of the connector **(Required)**|string||
|tasks.max|The number of tasks to scale output **(Required)**|int||
|connect.rabbitmq.kcql|KCQL Expression.Examples shown above **(Required)** |string||
|connect.rabbitmq.host|The RabbitMQ Server's IP **(Required)**|string||
|connect.rabbitmq.port|The RabbitMQ Server's Port|int|5672|
|connect.rabbitmq.username|Username|string|guest|
|connect.rabbitmq.password|Password|string|guest|
|connect.rabbitmq.virtual.host|Sets a custom endpoint for RabbitMQ|string|/|
|connect.rabbitmq.use.tls|Enables tls|boolean|true|

#### Source Specific 

|Config| Description | Type |Default|
|------|---------|---------|--|
|connector.class|Name of the connector class **(Required)**|string|com.datamountaineer.streamreactor.connect.rabbitmq.source.RabbitMQSourceConnector|
|connect.converter.avro.schemas|If the AvroConverter is used you need to provide an avro Schema to be able to read and translate the raw bytes to an avro record. The format is *$RABBITMQ_QUEUE=$PATH_TO_AVRO_SCHEMA_FILE*|string||
|connect.rabbitmq.polling.timeout|Timeout when polling messages (in ms)|long|1000|

###### Example Source configuration 

This configuration will connect to the queue named `rabbitmq_avro_queue` and forward the data (in avro format) from RabbitMQ to the 
`avro_kafka_topic` in Kafka. Since the AvroConverter is used, the file of the avro schema of the data also needs to be provided.

```
name=rabbitmq-source-connector                                                                                                                                                  
connector.class=com.datamountaineer.streamreactor.connect.rabbitmq.source.RabbitMQSourceConnector                                                                            
tasks.max=1
connect.rabbitmq.host=192.168.1.52                                                                                                                                            
connect.rabbitmq.port=5671                                                                                                                                                   
connect.rabbitmq.username=admin                                                                                                                                              
connect.rabbitmq.password=admin                                                                                                                                           
connect.source.converter.avro.schemas=rabbitmq_avro_queue=/home/user/rabbitmq-avro-schema.avro        
connect.rabbitmq.kcql=INSERT INTO avro_kafka_topic SELECT * FROM rabbitmq_avro_queue WITHCONVERTER=`com.datamountaineer.streamreactor.connect.converters.source.AvroConverter`
```

#### Sink Specific 

|Config| Description | Type |Default|
|------|---------|---------|--|
|connector.class|Name of the connector class **(Required)**|string|com.datamountaineer.streamreactor.connect.rabbitmq.sink.RabbitMQSinkConnector|
|topics|The topics to sink.The connector will cross check this with the KCQL statement **(Required)**|string||

###### Example Sink configuration 

This configuration will create two exchanges one with type *fanout* and one with type *topic* (if they don't already exist). It will
then forward all the messages from `kafka_topic01` to `exchange01` and from `kafka_topic02` to `exchange02` (using the routing key 
*my.routing.key* for the second exchange).

```
name=rabbitmq-sink-connector                                                                                                                                                  
connector.class=com.datamountaineer.streamreactor.connect.rabbitmq.sink.RabbitMQSinkConnector                                                                    
tasks.max=1
connect.rabbitmq.host=192.168.1.52                                                                                                                                            
connect.rabbitmq.port=5671    
connect.rabbitmq.username=admin                                                                                                                                              
connect.rabbitmq.password=admin
topics=kafka_topic01,kafka_topic02
connect.rabbitmq.kcql=INSERT INTO exchange01 SELECT * FROM kafka_topic01 WITHTYPE fanout;INSERT INTO exchange02 SELECT * FROM kafka_topic02 WITHTAG (my.routing.key) WITHTYPE topic
```
