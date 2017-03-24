package com.datamountaineer.streamreactor.connect

import java.io.{BufferedWriter, ByteArrayOutputStream, FileWriter}
import java.nio.file.Paths
import java.util.UUID
import javax.jms.{BytesMessage, Session, TextMessage}

import com.datamountaineer.streamreactor.connect.converters.source.AvroConverter
import com.datamountaineer.streamreactor.connect.jms.config.{DestinationSelector, JMSConfig}
import com.sksamuel.avro4s.{AvroOutputStream, SchemaFor}
import org.apache.activemq.jndi.ActiveMQInitialContextFactory
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 14/03/2017. 
  * stream-reactor
  */
case class Student(name: String, age: Int, note: Double)


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
  val CONNECTION_FACTORY = "ConnectionFactory"
  val INITIAL_CONTEXT_FACTORY = classOf[ActiveMQInitialContextFactory].getCanonicalName
  val JMS_URL = "tcp://localhost:61620"
  val JMS_URL_1 = "tcp://localhost:61621"
  val QUEUE_LIST = QUEUE1
  val TOPIC_LIST = TOPIC1
  val SELECTOR = DestinationSelector.CDI.toString
  val AVRO_QUEUE = "avro_queue"
  val KCQL_AVRO_SOURCE = s"INSERT INTO $TOPIC1 SELECT * FROM $AVRO_QUEUE"
  val KCQL_AVRO_SOURCE_MIX = s"$KCQL_AVRO_SOURCE;$KCQL_SOURCE_TOPIC"
  val QUEUE_CONVERTER = s"$AVRO_QUEUE=com.datamountaineer.streamreactor.connect.converters.source.AvroConverter"
  val AVRO_SCHEMA_CONFIG = s"${AVRO_QUEUE}=${getSchemaFile()}"

  def getSchemaFile(): String = {
    val schemaFile = Paths.get(UUID.randomUUID().toString)
    val schema = SchemaFor[Student]()
    val bw = new BufferedWriter(new FileWriter(schemaFile.toFile))
    bw.write(schema.toString)
    bw.close()
    schemaFile.toAbsolutePath.toString
  }

  def getProps1Queue(url: String = JMS_URL) = {
    Map(JMSConfig.KCQL -> KCQL_SOURCE_QUEUE,
      JMSConfig.JMS_USER -> JMS_USER,
      JMSConfig.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfig.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
      JMSConfig.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfig.JMS_URL -> url,
      JMSConfig.QUEUE_LIST -> QUEUE_LIST
    ).asJava
  }

  def getPropsBadFactory = {
    Map(JMSConfig.KCQL -> KCQL_SOURCE_QUEUE,
      JMSConfig.JMS_USER -> JMS_USER,
      JMSConfig.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfig.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
      JMSConfig.CONNECTION_FACTORY -> "plop",
      JMSConfig.JMS_URL -> JMS_URL,
      JMSConfig.QUEUE_LIST -> QUEUE_LIST
    ).asJava
  }

  def getProps1Topic = {
    Map(JMSConfig.KCQL -> KCQL_SOURCE_TOPIC,
      JMSConfig.JMS_USER -> JMS_USER,
      JMSConfig.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfig.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
      JMSConfig.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfig.JMS_URL -> JMS_URL,
      JMSConfig.TOPIC_LIST -> TOPIC_LIST
    ).asJava
  }

  def getProps1TopicJNDI = {
    Map(JMSConfig.KCQL -> KCQL_SOURCE_TOPIC,
      JMSConfig.JMS_USER -> JMS_USER,
      JMSConfig.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfig.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
      JMSConfig.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfig.JMS_URL -> JMS_URL,
      JMSConfig.TOPIC_LIST -> TOPIC_LIST,
      JMSConfig.DESTINATION_SELECTOR -> DestinationSelector.JNDI.toString
    ).asJava
  }

  def getPropsTopicListIncorrect = {
    Map(JMSConfig.KCQL -> KCQL_SOURCE_TOPIC,
      JMSConfig.JMS_USER -> JMS_USER,
      JMSConfig.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfig.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
      JMSConfig.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfig.JMS_URL -> JMS_URL,
      JMSConfig.TOPIC_LIST -> "foo",
      JMSConfig.DESTINATION_SELECTOR -> DestinationSelector.JNDI.toString
    ).asJava
  }

  def getPropsMixCDI(url : String = JMS_URL) = {
    Map(JMSConfig.KCQL -> KCQL_MIX,
      JMSConfig.JMS_USER -> JMS_USER,
      JMSConfig.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfig.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
      JMSConfig.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfig.JMS_URL -> url,
      JMSConfig.TOPIC_LIST -> TOPIC_LIST,
      JMSConfig.QUEUE_LIST -> QUEUE_LIST,
      JMSConfig.DESTINATION_SELECTOR -> DestinationSelector.CDI.toString
    ).asJava
  }

  def getPropsMixCDIWithConverters(url : String = JMS_URL) = {
    Map(JMSConfig.KCQL -> KCQL_AVRO_SOURCE_MIX,
      JMSConfig.JMS_USER -> JMS_USER,
      JMSConfig.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfig.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
      JMSConfig.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfig.JMS_URL -> url,
      JMSConfig.QUEUE_LIST -> AVRO_QUEUE,
      JMSConfig.TOPIC_LIST -> TOPIC1,
      JMSConfig.DESTINATION_SELECTOR -> DestinationSelector.CDI.toString,
      JMSConfig.CONVERTER_CONFIG -> QUEUE_CONVERTER,
      AvroConverter.SCHEMA_CONFIG -> AVRO_SCHEMA_CONFIG
    ).asJava
  }

  def getPropsMixJNDI(url : String = JMS_URL) = {
    Map(JMSConfig.KCQL -> KCQL_MIX,
      JMSConfig.JMS_USER -> JMS_USER,
      JMSConfig.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfig.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
      JMSConfig.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfig.JMS_URL -> url,
      JMSConfig.TOPIC_LIST -> TOPIC_LIST,
      JMSConfig.QUEUE_LIST -> QUEUE_LIST,
      JMSConfig.DESTINATION_SELECTOR -> DestinationSelector.JNDI.toString
    ).asJava
  }

  def getPropsMixWithConverter = {
    Map(JMSConfig.KCQL -> KCQL_AVRO_SOURCE_MIX,
      JMSConfig.JMS_USER -> JMS_USER,
      JMSConfig.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfig.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
      JMSConfig.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfig.JMS_URL -> JMS_URL,
      JMSConfig.TOPIC_LIST -> TOPIC_LIST,
      JMSConfig.QUEUE_LIST -> AVRO_QUEUE,
      JMSConfig.DESTINATION_SELECTOR -> SELECTOR,
      JMSConfig.CONVERTER_CONFIG -> QUEUE_CONVERTER,
      AvroConverter.SCHEMA_CONFIG -> AVRO_SCHEMA_CONFIG
    ).asJava
  }

  def getPropsQueueWithConverter(url: String = JMS_URL) = {
    Map(JMSConfig.KCQL -> KCQL_SOURCE_QUEUE,
      JMSConfig.JMS_USER -> JMS_USER,
      JMSConfig.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfig.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
      JMSConfig.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfig.JMS_URL -> url,
      JMSConfig.TOPIC_LIST -> TOPIC_LIST,
      JMSConfig.QUEUE_LIST -> QUEUE_LIST,
      JMSConfig.DESTINATION_SELECTOR -> SELECTOR,
      JMSConfig.CONVERTER_CONFIG -> QUEUE_CONVERTER
    ).asJava
  }

  def getPropsMixJNDIWithSink(url: String = JMS_URL) = {
    Map(JMSConfig.KCQL -> KCQL_MIX_SINK,
      JMSConfig.JMS_USER -> JMS_USER,
      JMSConfig.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfig.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
      JMSConfig.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfig.JMS_URL -> url,
      JMSConfig.TOPIC_LIST -> TOPIC_LIST,
      JMSConfig.QUEUE_LIST -> QUEUE_LIST,
      JMSConfig.DESTINATION_SELECTOR -> SELECTOR
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


  def getTextMessages(n : Int, session: Session) : Seq[TextMessage] = {
    (1 to n).map( i => session.createTextMessage(s"Message $i"))
  }

  def getBytesMessage(n : Int, session: Session) : Seq[BytesMessage] = {
    (1 to n).map( i => {
      val s = Student(s"andrew", i, i.toDouble)
      val msg = session.createBytesMessage()
      msg.writeBytes(getAvro(s))
      msg
    })
  }

  def getAvro(s : Student): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val output = AvroOutputStream.binary[Student](baos)
    output.write(s)
    output.close()
    baos.toByteArray
  }
 }
