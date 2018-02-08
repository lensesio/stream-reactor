/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect

import java.io.{BufferedWriter, ByteArrayOutputStream, File, FileWriter}
import java.net.ServerSocket
import java.nio.file.Paths
import java.util
import java.util.UUID
import javax.jms.{BytesMessage, Connection, Session, TextMessage}

import com.datamountaineer.streamreactor.connect.converters.source.AvroConverter
import com.datamountaineer.streamreactor.connect.jms.config.{DestinationSelector, JMSConfigConstants}
import com.sksamuel.avro4s.{AvroOutputStream, SchemaFor}
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.broker.{BrokerService, ConnectionContext}
import org.apache.activemq.command.Message
import org.apache.activemq.jndi.ActiveMQInitialContextFactory
import org.apache.activemq.security.MessageAuthorizationPolicy
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.mockito.MockitoSugar
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
  val KCQL_SOURCE_QUEUE = s"INSERT INTO $TOPIC1 SELECT * FROM $QUEUE1 WITHTYPE QUEUE"
  val KCQL_SINK_QUEUE = s"INSERT INTO $QUEUE1 SELECT * FROM $TOPIC2 WITHTYPE QUEUE"
  val KCQL_SOURCE_TOPIC = s"INSERT INTO $TOPIC1 SELECT * FROM $TOPIC1 WITHTYPE TOPIC"
  val KCQL_SINK_TOPIC = s"INSERT INTO $TOPIC1 SELECT * FROM $TOPIC1 WITHTYPE TOPIC"
  val KCQL_MIX = s"$KCQL_SOURCE_QUEUE;$KCQL_SOURCE_TOPIC"
  val KCQL_MIX_SINK = s"$KCQL_SINK_QUEUE;$KCQL_SINK_TOPIC"
  val MESSAGE_SELECTOR = "a > b"
  val KCQL_SOURCE_TOPIC_WITH_JMS_SELECTOR = kcqlWithMessageSelector(MESSAGE_SELECTOR)
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
  val QUEUE_CONVERTER = s"`com.datamountaineer.streamreactor.connect.converters.source.AvroConverter`"
  val KCQL_AVRO_SOURCE = s"INSERT INTO $TOPIC1 SELECT * FROM $AVRO_QUEUE WITHTYPE QUEUE WITHCONVERTER=$QUEUE_CONVERTER"
  val KCQL_AVRO_SOURCE_MIX = s"$KCQL_AVRO_SOURCE;$KCQL_SOURCE_TOPIC"

  val AVRO_FILE = getSchemaFile()
  val AVRO_SCHEMA_CONFIG = s"${AVRO_QUEUE}=${AVRO_FILE}"

  def getSchemaFile(): String = {
    val schemaFile = Paths.get(UUID.randomUUID().toString)
    val schema = SchemaFor[Student]()
    val bw = new BufferedWriter(new FileWriter(schemaFile.toFile))
    bw.write(schema.toString)
    bw.close()
    schemaFile.toAbsolutePath.toString
  }

  def getProps1Queue(url: String = JMS_URL) = {
    Map(
      "topics" -> QUEUE1,
      JMSConfigConstants.KCQL -> KCQL_SOURCE_QUEUE,
      JMSConfigConstants.JMS_USER -> JMS_USER,
      JMSConfigConstants.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfigConstants.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
      JMSConfigConstants.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfigConstants.JMS_URL -> url
    ).asJava
  }

  def getPropsBadFactory = {
    Map(JMSConfigConstants.KCQL -> KCQL_SOURCE_QUEUE,
      JMSConfigConstants.JMS_USER -> JMS_USER,
      JMSConfigConstants.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfigConstants.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
      JMSConfigConstants.CONNECTION_FACTORY -> "plop",
      JMSConfigConstants.JMS_URL -> JMS_URL
    ).asJava
  }

  def getProps1Topic(url: String = JMS_URL) = {
    Map(JMSConfigConstants.KCQL -> KCQL_SOURCE_TOPIC,
      JMSConfigConstants.JMS_USER -> JMS_USER,
      JMSConfigConstants.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfigConstants.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
      JMSConfigConstants.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfigConstants.JMS_URL -> url
    ).asJava
  }

  def getProps1TopicWithMessageSelector(url: String = JMS_URL, msgSelector: String = MESSAGE_SELECTOR) = {
    getProps1Topic(url).asScala ++
      Map(JMSConfigConstants.KCQL -> kcqlWithMessageSelector(msgSelector),
        JMSConfigConstants.JMS_URL -> url)
  }.asJava

  def getProps1TopicJNDI = {
    Map(JMSConfigConstants.KCQL -> KCQL_SOURCE_TOPIC,
      JMSConfigConstants.JMS_USER -> JMS_USER,
      JMSConfigConstants.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfigConstants.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
      JMSConfigConstants.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfigConstants.JMS_URL -> JMS_URL,
      JMSConfigConstants.DESTINATION_SELECTOR -> DestinationSelector.JNDI.toString
    ).asJava
  }

  def getPropsTopicListIncorrect = {
    Map(JMSConfigConstants.KCQL -> s"INSERT INTO $TOPIC1 SELECT * FROM $TOPIC1",
      JMSConfigConstants.JMS_USER -> JMS_USER,
      JMSConfigConstants.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfigConstants.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
      JMSConfigConstants.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfigConstants.JMS_URL -> JMS_URL,
      JMSConfigConstants.DESTINATION_SELECTOR -> DestinationSelector.JNDI.toString
    ).asJava
  }

  def getPropsMixCDI(url: String = JMS_URL) = {
    Map(JMSConfigConstants.KCQL -> KCQL_MIX,
      JMSConfigConstants.JMS_USER -> JMS_USER,
      JMSConfigConstants.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfigConstants.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
      JMSConfigConstants.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfigConstants.JMS_URL -> url,
      JMSConfigConstants.DESTINATION_SELECTOR -> DestinationSelector.CDI.toString
    ).asJava
  }

  def getPropsMixCDIWithConverters(url: String = JMS_URL) = {
    Map(JMSConfigConstants.KCQL -> KCQL_AVRO_SOURCE_MIX,
      JMSConfigConstants.JMS_USER -> JMS_USER,
      JMSConfigConstants.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfigConstants.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
      JMSConfigConstants.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfigConstants.JMS_URL -> url,
      JMSConfigConstants.DESTINATION_SELECTOR -> DestinationSelector.CDI.toString,
      AvroConverter.SCHEMA_CONFIG -> AVRO_SCHEMA_CONFIG
    ).asJava
  }

  def getPropsMixJNDI(url: String = JMS_URL) = {
    Map(JMSConfigConstants.KCQL -> KCQL_MIX,
      JMSConfigConstants.JMS_USER -> JMS_USER,
      JMSConfigConstants.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfigConstants.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
      JMSConfigConstants.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfigConstants.JMS_URL -> url,
      JMSConfigConstants.DESTINATION_SELECTOR -> DestinationSelector.JNDI.toString
    ).asJava
  }

  def getPropsMixWithConverter = {
    Map(JMSConfigConstants.KCQL -> KCQL_AVRO_SOURCE_MIX,
      JMSConfigConstants.JMS_USER -> JMS_USER,
      JMSConfigConstants.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfigConstants.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
      JMSConfigConstants.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfigConstants.JMS_URL -> JMS_URL,
      JMSConfigConstants.DESTINATION_SELECTOR -> SELECTOR,
      AvroConverter.SCHEMA_CONFIG -> AVRO_SCHEMA_CONFIG
    ).asJava
  }

  def getPropsQueueWithConverter(url: String = JMS_URL) = {
    Map(JMSConfigConstants.KCQL -> KCQL_SOURCE_QUEUE,
      JMSConfigConstants.JMS_USER -> JMS_USER,
      JMSConfigConstants.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfigConstants.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
      JMSConfigConstants.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfigConstants.JMS_URL -> url,
      JMSConfigConstants.DESTINATION_SELECTOR -> SELECTOR
    ).asJava
  }

  def getPropsMixJNDIWithSink(url: String = JMS_URL) = {
    Map(JMSConfigConstants.KCQL -> KCQL_MIX_SINK,
      JMSConfigConstants.JMS_USER -> JMS_USER,
      JMSConfigConstants.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfigConstants.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
      JMSConfigConstants.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfigConstants.JMS_URL -> url,
      JMSConfigConstants.DESTINATION_SELECTOR -> SELECTOR
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

  def getSinkRecords = {
    List(new SinkRecord(TOPIC1, 0, null, null, getSchema, getStruct(getSchema), 1),
      new SinkRecord(TOPIC2, 0, null, null, getSchema, getStruct(getSchema), 5))
  }


  def getTextMessages(n: Int, session: Session): Seq[TextMessage] = {
    (1 to n).map(i => session.createTextMessage(s"Message $i"))
  }

  def getBytesMessage(n: Int, session: Session): Seq[BytesMessage] = {
    (1 to n).map(i => {
      val s = Student(s"andrew", i, i.toDouble)
      val msg = session.createBytesMessage()
      msg.writeBytes(getAvro(s))
      msg
    })
  }

  def getAvro(s: Student): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val output = AvroOutputStream.binary[Student](baos)
    output.write(s)
    output.close()
    baos.toByteArray
  }

  def kcqlWithMessageSelector(msgSelector: String) =
    s"INSERT INTO $TOPIC1 SELECT * FROM $JMS_TOPIC1 WITHTYPE TOPIC WITHJMSSELECTOR=`$msgSelector`"

  def getFreePort: Int = {
    val socket = new ServerSocket(0)
    socket.setReuseAddress(true)
    val port = socket.getLocalPort
    socket.close()
    port
  }

  def testWithBrokerOnPort(test: (Connection, String) => Unit): Unit = testWithBrokerOnPort()(test)

  def testWithBrokerOnPort(port: Int = getFreePort)(test: (Connection, String) => Unit): Unit =
    testWithBroker(port, None) { brokerUrl =>
      val connectionFactory = new ActiveMQConnectionFactory()
      connectionFactory.setBrokerURL(brokerUrl)
      val conn = connectionFactory.createConnection()
      conn.start()
      test(conn, brokerUrl)
    }

  def testWithBroker(port: Int = getFreePort, clientID: Option[String])(test: String => Unit): Unit = {
    val broker = new BrokerService()
    broker.setPersistent(false)
    broker.setUseJmx(false)
    broker.setDeleteAllMessagesOnStartup(true)
    val brokerUrl = s"tcp://localhost:$port${clientID.fold("")(id => s"?jms.clientID=$id")}"
    broker.addConnector(brokerUrl)
    broker.setUseShutdownHook(false)
    val property = "java.io.tmpdir"
    val tempDir = System.getProperty(property)
    broker.setDataDirectoryFile(new File(tempDir))
    broker.setTmpDataDirectory(new File(tempDir))
    broker.start()
    test(brokerUrl)
  }
}
