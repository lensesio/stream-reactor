package com.datamountaineer.streamreactor.connect

import com.datamountaineer.streamreactor.connect.jms.config.{DestinationSelector, JMSConfig}
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 14/03/2017. 
  * stream-reactor
  */
trait TestBase extends WordSpec with Matchers with MockitoSugar {
  val TOPIC1 = "topic1"
  val TOPIC2 = "topic2"
  val QUEUE1 = "queue1"
  val JMS_TOPIC1 = TOPIC1
  val KCQL_SOURCE_QUEUE = s"INSERT INTO $TOPIC1 SELECT * FROM $QUEUE1"
  val KCQL_SINK_QUEUE = s"INSERT INTO $QUEUE1 SELECT * FROM $TOPIC2"
  val KCQL_SOURCE_TOPIC = s"INSERT INTO $TOPIC1 SELECT * FROM $TOPIC1"
  val KCQL_SINK_TOPIC = s"INSERT INTO $TOPIC1 SELECT * FROM $TOPIC1"
  val KCQL_MIX = s"$KCQL_SOURCE_QUEUE;$KCQL_SOURCE_TOPIC"
  val KCQL_MIX_SINK = s"$KCQL_SINK_QUEUE;$KCQL_SINK_TOPIC"
  val JMS_USER = ""
  val JMS_PASSWORD = ""
  val CONNECTION_FACTORY = classOf[ActiveMQConnectionFactory].getCanonicalName
  val JMS_URL = "tcp://localhost:61620"
  val JMS_URL_1 = "tcp://localhost:61621"
  val QUEUE_LIST = QUEUE1
  val TOPIC_LIST = TOPIC1
  val TOPIC1_CONVERTER = s"$TOPIC1=com.datamountaineer.streamreactor.connect.converters.source.AvroConverter"

  val getProps1Queue = {
    Map(JMSConfig.KCQL -> KCQL_SOURCE_QUEUE,
      JMSConfig.JMS_USER -> JMS_USER,
      JMSConfig.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfig.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfig.JMS_URL -> JMS_URL,
      JMSConfig.QUEUE_LIST -> QUEUE_LIST
    ).asJava
  }

  val getPropsBadFactory = {
    Map(JMSConfig.KCQL -> KCQL_SOURCE_QUEUE,
      JMSConfig.JMS_USER -> JMS_USER,
      JMSConfig.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfig.CONNECTION_FACTORY -> "plop",
      JMSConfig.JMS_URL -> JMS_URL,
      JMSConfig.QUEUE_LIST -> QUEUE_LIST
    ).asJava
  }

  val getProps1Topic = {
    Map(JMSConfig.KCQL -> KCQL_SOURCE_TOPIC,
      JMSConfig.JMS_USER -> JMS_USER,
      JMSConfig.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfig.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfig.JMS_URL -> JMS_URL,
      JMSConfig.TOPIC_LIST -> TOPIC_LIST
    ).asJava
  }

  val getProps1TopicJNDI = {
    Map(JMSConfig.KCQL -> KCQL_SOURCE_TOPIC,
      JMSConfig.JMS_USER -> JMS_USER,
      JMSConfig.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfig.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfig.JMS_URL -> JMS_URL,
      JMSConfig.TOPIC_LIST -> TOPIC_LIST,
      JMSConfig.DESTINATION_SELECTOR -> DestinationSelector.JNDI.toString
    ).asJava
  }

  val getPropsTopicListIncorrect = {
    Map(JMSConfig.KCQL -> KCQL_SOURCE_TOPIC,
      JMSConfig.JMS_USER -> JMS_USER,
      JMSConfig.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfig.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfig.JMS_URL -> JMS_URL,
      JMSConfig.TOPIC_LIST -> "foo",
      JMSConfig.DESTINATION_SELECTOR -> DestinationSelector.JNDI.toString
    ).asJava
  }

  val getPropsMixJNDI = {
    Map(JMSConfig.KCQL -> KCQL_MIX,
      JMSConfig.JMS_USER -> JMS_USER,
      JMSConfig.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfig.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfig.JMS_URL -> JMS_URL,
      JMSConfig.TOPIC_LIST -> TOPIC_LIST,
      JMSConfig.QUEUE_LIST -> QUEUE_LIST,
      JMSConfig.DESTINATION_SELECTOR -> DestinationSelector.JNDI.toString
    ).asJava
  }

  val getPropsMixJNDIWithConverter = {
    Map(JMSConfig.KCQL -> KCQL_MIX,
      JMSConfig.JMS_USER -> JMS_USER,
      JMSConfig.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfig.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfig.JMS_URL -> JMS_URL,
      JMSConfig.TOPIC_LIST -> TOPIC_LIST,
      JMSConfig.QUEUE_LIST -> QUEUE_LIST,
      JMSConfig.DESTINATION_SELECTOR -> DestinationSelector.JNDI.toString,
      JMSConfig.CONVERTER_CONFIG -> TOPIC1_CONVERTER
    ).asJava
  }

  def getPropsMixJNDIWithConverterSink(url: String = JMS_URL) = {
    Map(JMSConfig.KCQL -> KCQL_MIX_SINK,
      JMSConfig.JMS_USER -> JMS_USER,
      JMSConfig.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfig.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfig.JMS_URL -> url,
      JMSConfig.TOPIC_LIST -> TOPIC_LIST,
      JMSConfig.QUEUE_LIST -> QUEUE_LIST,
      JMSConfig.DESTINATION_SELECTOR -> DestinationSelector.JNDI.toString,
      JMSConfig.CONVERTER_CONFIG -> TOPIC1_CONVERTER
    ).asJava
  }

  def getSchema: Schema = {
    SchemaBuilder.struct
      .field("int8", SchemaBuilder.int8().defaultValue(2.toByte).doc("int8 field").build())
      .field("int16", Schema.INT16_SCHEMA)
      .field("int32", Schema.INT32_SCHEMA)
      .field("int64", Schema.INT64_SCHEMA)
      .field("float32", Schema.FLOAT32_SCHEMA)
      .field("float64", Schema.FLOAT64_SCHEMA)
      .field("boolean", Schema.BOOLEAN_SCHEMA)
      .field("string", Schema.STRING_SCHEMA)
      .field("bytes", Schema.BYTES_SCHEMA)
      .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
      .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
      .field("mapNonStringKeys", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build())
      .build()
  }


  def getStruct(schema: Schema): Struct = {
    new Struct(schema)
      .put("int8", 12.toByte)
      .put("int16", 12.toShort)
      .put("int32", 12)
      .put("int64", 12L)
      .put("float32", 12.2f)
      .put("float64", 12.2)
      .put("boolean", true)
      .put("string", "foo")
      .put("bytes", "foo".getBytes())
      .put("array", List("a", "b", "c").asJava)
      .put("map", Map("field" -> 1).asJava)
      .put("mapNonStringKeys", Map(1 -> 1).asJava)
  }

  def getSinkRecords  = {
    List(new SinkRecord(TOPIC1, 0, null, null, getSchema, getStruct(getSchema), 1),
        new SinkRecord(TOPIC2, 0, null, null, getSchema, getStruct(getSchema), 5))
  }
}
