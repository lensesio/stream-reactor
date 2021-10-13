/*
 *
 *  * Copyright 2020 Lenses.io.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.datamountaineer.streamreactor.connect.jms


import com.datamountaineer.streamreactor.connect.jms.config.{DestinationSelector, JMSConfigConstants}
import com.sksamuel.avro4s.{AvroOutputStream, SchemaFor}
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.broker.BrokerService
import org.apache.activemq.jndi.ActiveMQInitialContextFactory
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.{BufferedWriter, ByteArrayOutputStream, File, FileWriter}
import java.net.ServerSocket
import java.nio.file.Paths
import java.util
import java.util.UUID
import javax.jms.{BytesMessage, Connection, Session, TextMessage}
import scala.collection.JavaConverters._

/**
 * Created by andrew@datamountaineer.com on 14/03/2017.
 * stream-reactor
 */
case class Student(name: String, age: Int, note: Double)


trait TestBase extends AnyWordSpec with Matchers with MockitoSugar {

  val MESSAGE_SELECTOR = "a > b"
  val JMS_USER = ""
  val JMS_PASSWORD = ""
  val CONNECTION_FACTORY = "ConnectionFactory"
  val INITIAL_CONTEXT_FACTORY = classOf[ActiveMQInitialContextFactory].getCanonicalName
  val JMS_URL = "tcp://localhost:61620"
  val JMS_URL_1 = "tcp://localhost:61621"
  val AVRO_QUEUE = "avro_queue"
  val QUEUE_CONVERTER = s"`com.datamountaineer.streamreactor.connect.converters.source.AvroConverter`"
  val QUEUE_CONVERTER_JMS = s"`com.datamountaineer.streamreactor.connect.jms.sink.converters.ProtoMessageConverter`"
  val FORMAT = "AVRO"
  val PROTO_FORMAT = "PROTOBUF"
  val SUBSCRIPTION_NAME = "subscriptionName"
  val AVRO_FILE = getSchemaFile()


  def getAvroProp(topic: String) = s"${topic}=${AVRO_FILE}"
  def getKCQL(target: String, source: String, jmsType: String) = s"INSERT INTO $target SELECT * FROM $source WITHTYPE $jmsType"
  def getKCQLAvroSinkConverter(target: String, source: String, jmsType: String) = s"INSERT INTO $target SELECT * FROM $source WITHTYPE $jmsType WITHCONVERTER=$QUEUE_CONVERTER_JMS"

  def getKCQLFormat(target: String, source: String, jmsType: String, format: String) = s"INSERT INTO $target SELECT * FROM $source WITHFORMAT $format WITHTYPE $jmsType"
  def getKCQLStoreAsAddressedPerson(target: String, source: String, jmsType: String) = s"INSERT INTO $target SELECT * FROM $source  STOREAS `datamountaineer.streamreactor.example.AddressedPerson` WITHTYPE $jmsType"
  def getKCQLEmptyStoredAsNonAddressedPerson(target: String, source: String, jmsType: String) = s"INSERT INTO $target SELECT * FROM $source STOREAS `datamountaineer.streamreactor.example.NonAddressedPerson` WITHTYPE $jmsType"
  def getKCQLStoreAsTimedPerson(target: String, source: String, jmsType: String, path: String) = s"INSERT INTO $target SELECT * FROM $source STOREAS `datamountaineer.streamreactor.example.TimedPerson`(proto_path = $path, proto_file = `$path/TimedPerson.proto`) WITHTYPE $jmsType WITHFORMAT $PROTO_FORMAT"
  def getKCQLStoreAsWithFileAndPath(target: String, source: String, jmsType: String, file: String, path: String) = s"INSERT INTO $target SELECT col1,col2 FROM $source STOREAS `datamountaineer.streamreactor.example.NonAddressedPerson`(proto_path = $path, proto_file = $file) WITHTYPE $jmsType"
  def getKCQLStoredAsWithNameOnly(target: String, source: String, jmsType: String) = s"INSERT INTO $target SELECT * FROM $source STOREAS `com.datamountaineer.streamreactor.example.NonAddressedPersonOuterClass`  WITHTYPE $jmsType"
  def getKCQLStoredAsWithInvalidData(target: String, source: String, jmsType: String) = s"INSERT INTO $target SELECT col1,col2 FROM $source STOREAS NonAddressedPersonOuterClass  WITHTYPE $jmsType"
  def getKCQLStoredAsWithInvalidPackageNameWithProtopath(target: String, source: String, jmsType: String, path: String) = s"INSERT INTO $target SELECT col1,col2 FROM $source STOREAS NonAddressedPersonOuterClass(proto_path = $path)  WITHTYPE $jmsType"
  def getKCQLStoredAsWithProtopath(target: String, source: String, jmsType: String, path: String) = s"INSERT INTO $target SELECT col1,col2 FROM $source STOREAS `datamountaineer.streamreactor.example.alien.AlienPerson`(proto_path = $path)  WITHTYPE $jmsType"
  def getKCQLAvroSource(topic: String, queue: String, jmsType: String) = s"INSERT INTO $topic SELECT * FROM $queue WITHTYPE $jmsType WITHCONVERTER=$QUEUE_CONVERTER WITHSUBSCRIPTION=$SUBSCRIPTION_NAME"

  def getSchemaFile(): String = {
    val schemaFile = Paths.get(UUID.randomUUID().toString)
    val schema = SchemaFor[Student]()
    val bw = new BufferedWriter(new FileWriter(schemaFile.toFile))
    bw.write(schema.toString)
    bw.close()
    schemaFile.toAbsolutePath.toString
  }

  def getSinkProps(kcql: String, topics: String,
                   url: String, customProperties: Map[String, String] = Map()): util.Map[String, String] = {
    (Map("topics" -> topics) ++ getProps(kcql, url) ++ customProperties).asJava
  }

  def getProps(kcql: String, url: String): Map[String, String] = {
    Map(
      JMSConfigConstants.KCQL -> kcql,
      JMSConfigConstants.JMS_USER -> JMS_USER,
      JMSConfigConstants.JMS_PASSWORD -> JMS_PASSWORD,
      JMSConfigConstants.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
      JMSConfigConstants.CONNECTION_FACTORY -> CONNECTION_FACTORY,
      JMSConfigConstants.JMS_URL -> url,
      JMSConfigConstants.DESTINATION_SELECTOR -> DestinationSelector.CDI.toString
    )
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

  def getProtobufSchema: Schema = {
    SchemaBuilder.struct
      .field("name", Schema.STRING_SCHEMA)
      .field("id", Schema.INT32_SCHEMA)
      .field("email", Schema.STRING_SCHEMA)
      .build()
  }

  def getProtobufStruct(schema: Schema, name: String, id: Int, email:String): Struct = {
    new Struct(schema)
      .put("name", name)
      .put("id", id)
      .put("email", email)
  }

  def getProtobufSchemaTimestamp: Schema = {
    SchemaBuilder.struct
      .field("name", Schema.STRING_SCHEMA)
      .field("id", Schema.INT32_SCHEMA)
      .field("timestamp", Schema.STRING_SCHEMA)
      .build()
  }

  def getProtobufStructTimestamp(schema: Schema, name: String, id: Int, timeStamp:String): Struct = {
    new Struct(schema)
      .put("name", name)
      .put("id", id)
      .put("timestamp", timeStamp)
  }

  def getSinkRecords(topic: String) = {
    List(new SinkRecord(topic, 0, null, null, getSchema, getStruct(getSchema), 1),
      new SinkRecord(topic, 0, null, null, getSchema, getStruct(getSchema), 5))
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

  def kcqlWithMessageSelector(target: String, source: String, msgSelector: String) =
    s"INSERT INTO $target SELECT * FROM $source WITHTYPE TOPIC WITHJMSSELECTOR=`$msgSelector`"

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

  def testWithBroker(port: Int = getFreePort, clientID: Option[String])(test: String => Unit): BrokerService = {
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
    broker
  }
}