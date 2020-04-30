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

package com.datamountaineer.streamreactor.connect.mqtt.source

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.ByteBuffer
import java.nio.file.Paths
import java.util
import java.util.UUID

import com.datamountaineer.streamreactor.connect.converters.MsgKey
import com.datamountaineer.streamreactor.connect.converters.source._
import com.datamountaineer.streamreactor.connect.mqtt.config.{MqttConfigConstants, MqttSourceConfig, MqttSourceSettings}
import com.datamountaineer.streamreactor.connect.mqtt.connection.MqttClientConnectionFn
import com.datamountaineer.streamreactor.connect.serialization.AvroSerializer
import com.sksamuel.avro4s.{RecordFormat, SchemaFor}
import io.confluent.connect.avro.AvroData
import io.moquette.proto.messages.{AbstractMessage, PublishMessage}
import io.moquette.server.Server
import io.moquette.server.config.ClasspathConfig
import org.apache.kafka.connect.data.{Schema, Struct}
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.JavaConverters._
import scala.util.Try

class MqttManagerTest extends AnyWordSpec with Matchers with BeforeAndAfter {

  val classPathConfig = new ClasspathConfig()
  val connection = "tcp://0.0.0.0:1883"
  val clientId = "MqttManagerTest"
  val clientPersistentId = "MqttManagerTestDisableClean"
  val qs = 1
  val connectionTimeout = 1000
  val pollingTimeout = 500
  val keepAlive = 1000

  var mqttBroker: Option[Server] = None

  before {
    startNewMockMqttBroker()
  }

  after {
    stopMockMqttBroker()
  }

  private def startNewMockMqttBroker(): Unit ={
    mqttBroker = Some(new Server())
    mqttBroker.foreach(_.startServer(classPathConfig))
  }

  private def stopMockMqttBroker(): Unit ={
    mqttBroker.foreach {
      _.stopServer()
    }

    val files = Seq("moquette_store.mapdb", "moquette_store.mapdb.p", "moquette_store.mapdb.t")
    files.map(f => new File(f)).withFilter(_.exists()).foreach { f => Try(f.delete()) }
  }

  private def initializeConverter(mqttSource: String, converter: AvroConverter, schema: org.apache.avro.Schema) = {
    val schemaFile = Paths.get(UUID.randomUUID().toString)

    def writeSchema(schema: org.apache.avro.Schema): File = {

      val bw = new BufferedWriter(new FileWriter(schemaFile.toFile))
      bw.write(schema.toString)
      bw.close()

      schemaFile.toFile
    }

    try {
      converter.initialize(Map(
        AvroConverter.SCHEMA_CONFIG -> s"$mqttSource=${writeSchema(schema)}"
      ))
    }
    finally {
      schemaFile.toFile.delete()
    }

  }


  "MqttManager" should {

    "dynamically set the target kafka topic to the topic from the mqtt topic on wildcards" in {
      val source = "/mqttSourceTopic/+/test"
      val target = "`$`"
      val sourcesToConvMap = Map(source -> new BytesConverter)
      val settings = MqttSourceSettings(
        connection,
        None,
        None,
        clientId,
        sourcesToConvMap.map { case (k, v) => k -> v.getClass.getCanonicalName },
        true,
        Array(s"INSERT INTO $target SELECT * FROM $source"),
        qs,
        connectionTimeout,
        pollingTimeout,
        true,
        keepAlive,
        None,
        None,
        None
        )
      val mqttManager = new MqttManager(MqttClientConnectionFn.apply,
        sourcesToConvMap,
        settings)
      Thread.sleep(2000)

      val messages = Seq("message1", "message2")

      publishMessage("/mqttSourceTopic/A/test", messages.head.getBytes)
      publishMessage("/mqttSourceTopic/B/test", messages.last.getBytes)

      Thread.sleep(3000)

      var records = new util.LinkedList[SourceRecord]()
      mqttManager.getRecords(records)

      try {
        records.size() shouldBe 2
        records.get(0).topic() shouldBe "mqttSourceTopic_A_test"
        records.get(1).topic() shouldBe "mqttSourceTopic_B_test"

        records.get(0).value() shouldBe messages(0).getBytes()
        records.get(1).value() shouldBe messages(1).getBytes()


        records.get(0).valueSchema() shouldBe Schema.BYTES_SCHEMA
        records.get(1).valueSchema() shouldBe Schema.BYTES_SCHEMA

        val msg3 = "message3".getBytes
        publishMessage("/mqttSourceTopic/C/test", msg3)

        Thread.sleep(2000)

        records = new util.LinkedList[SourceRecord]()
        mqttManager.getRecords(records)

        records.size() shouldBe 1
        records.get(0).topic() shouldBe "mqttSourceTopic_C_test"
        records.get(0).value() shouldBe msg3
        records.get(0).valueSchema() shouldBe Schema.BYTES_SCHEMA
      }
      finally {
        mqttManager.close()
      }
    }


    "process the messages on topic A and create source records with Bytes schema with Wildcards" in {
      val source = "/mqttSourceTopic/+/test"
      val target = "kafkaTopic"
      val sourcesToConvMap = Map(source -> new BytesConverter)
      val settings = MqttSourceSettings(
        connection,
        None,
        None,
        clientId,
        sourcesToConvMap.map { case (k, v) => k -> v.getClass.getCanonicalName },
        true,
        Array(s"INSERT INTO $target SELECT * FROM $source"),
        qs,
        connectionTimeout,
        pollingTimeout,
        true,
        keepAlive,
        None,
        None,
        None
      )
      val mqttManager = new MqttManager(MqttClientConnectionFn.apply,
        sourcesToConvMap,
        settings)
      Thread.sleep(2000)

      val messages = Seq("message1", "message2")

      publishMessage("/mqttSourceTopic/A/test", messages.head.getBytes)
      publishMessage("/mqttSourceTopic/B/test", messages.last.getBytes)

      Thread.sleep(3000)

      var records = new util.LinkedList[SourceRecord]()
      mqttManager.getRecords(records)

      try {
        records.size() shouldBe 2
        records.get(0).topic() shouldBe target
        records.get(1).topic() shouldBe target

        records.get(0).value() shouldBe messages(0).getBytes()
        records.get(1).value() shouldBe messages(1).getBytes()


        records.get(0).valueSchema() shouldBe Schema.BYTES_SCHEMA
        records.get(1).valueSchema() shouldBe Schema.BYTES_SCHEMA

        val msg3 = "message3".getBytes
        publishMessage("/mqttSourceTopic/C/test", msg3)

        Thread.sleep(2000)

        records = new util.LinkedList[SourceRecord]()
        mqttManager.getRecords(records)

        records.size() shouldBe 1
        records.get(0).topic() shouldBe target
        records.get(0).value() shouldBe msg3
        records.get(0).valueSchema() shouldBe Schema.BYTES_SCHEMA
      }
      finally {
        mqttManager.close()
      }
    }


    "process the messages on topic A and create source records with Bytes schema" in {
      val source = "/mqttSourceTopic"
      val target = "kafkaTopic"
      val sourcesToConvMap = Map(source -> new BytesConverter)
      val settings = MqttSourceSettings(
        connection,
        None,
        None,
        clientId,
        sourcesToConvMap.map { case (k, v) => k -> v.getClass.getCanonicalName },
        true,
        Array(s"INSERT INTO $target SELECT * FROM $source"),
        qs,
        connectionTimeout,
        pollingTimeout,
        true,
        keepAlive,
        None,
        None,
        None
      )
      val mqttManager = new MqttManager(MqttClientConnectionFn.apply,
        sourcesToConvMap,
        settings)
      Thread.sleep(2000)

      val messages = Seq("message1", "message2")
      messages.foreach { m =>
        publishMessage(source, m.getBytes())
      }

      Thread.sleep(2000)

      var records = new util.LinkedList[SourceRecord]()
      mqttManager.getRecords(records)

      try {
        records.size() shouldBe 2
        records.get(0).topic() shouldBe target
        records.get(1).topic() shouldBe target

        records.get(0).value() shouldBe messages(0).getBytes()
        records.get(1).value() shouldBe messages(1).getBytes()


        records.get(0).valueSchema() shouldBe Schema.BYTES_SCHEMA
        records.get(1).valueSchema() shouldBe Schema.BYTES_SCHEMA

        val msg3 = "message3".getBytes
        publishMessage(source, msg3)

        Thread.sleep(500)

        records = new util.LinkedList[SourceRecord]()
        mqttManager.getRecords(records)

        records.size() shouldBe 1
        records.get(0).topic() shouldBe target
        records.get(0).value() shouldBe msg3
        records.get(0).valueSchema() shouldBe Schema.BYTES_SCHEMA
      }
      finally {
        mqttManager.close()
      }

    }

    "process the messages published before subscribing" in {
      val source = "/mqttSourceTopic"
      val target = "kafkaTopic"
      val sourcesToConvMap = Map(source -> new BytesConverter)
      val settings = MqttSourceSettings(
        connection,
        None,
        None,
        clientPersistentId,
        sourcesToConvMap.map { case (k, v) => k -> v.getClass.getCanonicalName },
        true,
        Array(s"INSERT INTO $target SELECT * FROM $source"),
        qs,
        connectionTimeout,
        pollingTimeout,
        cleanSession = false,
        keepAlive,
        None,
        None,
        None
      )

      val mqttManagerTmp = new MqttManager(MqttClientConnectionFn.apply,
        sourcesToConvMap,
        settings)
      Thread.sleep(2000)
      mqttManagerTmp.close()
      Thread.sleep(2000)

      val messages = Seq("message1")
      messages.foreach { m =>
        publishMessage(source, m.getBytes())
      }
      Thread.sleep(2000)

      val mqttManager = new MqttManager(MqttClientConnectionFn.apply,
        sourcesToConvMap,
        settings)
      Thread.sleep(2000)

      var records = new util.LinkedList[SourceRecord]()
      mqttManager.getRecords(records)

      try {
        records.size() shouldBe 1
        records.get(0).topic() shouldBe target
        records.get(0).value() shouldBe messages(0).getBytes()
        records.get(0).valueSchema() shouldBe Schema.BYTES_SCHEMA

      }
      finally {
        mqttManager.close()
      }

    }

    "handle each mqtt source based on the converter" in {
      val source1 = "/mqttSource1"
      val source2 = "/mqttSource2"
      val source3 = "/mqttSource3"

      val target1 = "kafkaTopic1"
      val target2 = "kafkaTopic2"
      val target3 = "kafkaTopic3"

      val avroConverter = new AvroConverter
      val studentSchema = SchemaFor[Student]()
      initializeConverter(source3, avroConverter, studentSchema)
      val sourcesToConvMap: Map[String, Converter] = Map(source1 -> new BytesConverter,
        source2 -> new JsonSimpleConverter,
        source3 -> avroConverter)

      val settings = MqttSourceSettings(
        connection,
        None,
        None,
        clientId,
        sourcesToConvMap.map { case (k, v) => k -> v.getClass.getCanonicalName },
        true,
        Array(s"INSERT INTO $target1 SELECT * FROM $source1",
          s"INSERT INTO $target2 SELECT * FROM $source2",
          s"INSERT INTO $target3 SELECT * FROM $source3"),
        qs,
        connectionTimeout,
        pollingTimeout,
        cleanSession = true,
        keepAlive,
        None,
        None,
        None
      )

      val mqttManager = new MqttManager(MqttClientConnectionFn.apply,
        sourcesToConvMap,
        settings)
      Thread.sleep(2000)

      val message1 = "message1".getBytes()


      val student = Student("Mike Bush", 19, 9.3)
      val message2 = JacksonJson.toJson(student).getBytes

      val recordFormat = RecordFormat[Student]

      val message3 = AvroSerializer.getBytes(student)

      publishMessage(source1, message1)
      publishMessage(source2, message2)
      publishMessage(source3, message3)
      Thread.sleep(3000)
      val records = new util.LinkedList[SourceRecord]()
      mqttManager.getRecords(records)

      try {
        records.size() shouldBe 3

        val avroData = new AvroData(4)

        records.asScala.foreach { record =>

          record.keySchema() shouldBe MsgKey.schema
          val source = record.key().asInstanceOf[Struct].get("topic")
          record.topic() match {
            case `target1` =>
              source shouldBe source1
              record.valueSchema() shouldBe Schema.BYTES_SCHEMA
              record.value() shouldBe message1

            case `target2` =>
              source shouldBe source2

              record.valueSchema().field("name").schema() shouldBe Schema.STRING_SCHEMA
              record.valueSchema().field("name").index() shouldBe 0
              record.valueSchema().field("age").schema() shouldBe Schema.INT64_SCHEMA
              record.valueSchema().field("age").index() shouldBe 1
              record.valueSchema().field("note").schema() shouldBe Schema.FLOAT64_SCHEMA
              record.valueSchema().field("note").index() shouldBe 2

              val struct = record.value().asInstanceOf[Struct]
              struct.getString("name") shouldBe student.name
              struct.getInt64("age") shouldBe student.age.toLong
              struct.getFloat64("note") shouldBe student.note

            case `target3` =>
              source shouldBe source3

              record.valueSchema().field("name").schema() shouldBe Schema.STRING_SCHEMA
              record.valueSchema().field("name").index() shouldBe 0
              record.valueSchema().field("age").schema() shouldBe Schema.INT32_SCHEMA
              record.valueSchema().field("age").index() shouldBe 1
              record.valueSchema().field("note").schema() shouldBe Schema.FLOAT64_SCHEMA
              record.valueSchema().field("note").index() shouldBe 2

              val struct = record.value().asInstanceOf[Struct]
              struct.getString("name") shouldBe student.name
              struct.getInt32("age") shouldBe student.age
              struct.getFloat64("note") shouldBe student.note

            case other => fail(s"$other is not a valid topic")
          }

        }
      }
      finally {
        mqttManager.close()
      }
    }
    "resubscribe after losing the connection with the broker" in {
      val source = "mqttSourceTopic"
      val target = "kafkaTopic"
      val sourcesToConvMap = Map(source -> new BytesConverter)
      val settings = MqttSourceSettings(
        connection,
        None,
        None,
        clientId,
        sourcesToConvMap.map { case (k, v) => k -> v.getClass.getCanonicalName },
        true,
        Array(s"INSERT INTO $target SELECT * FROM $source"),
        qs,
        connectionTimeout,
        pollingTimeout,
        true,
        keepAlive,
        None,
        None,
        None
      )

      // Instantiate the mqtt manager. It will subscribe to topic $source, at the currently active broker.
      val mqttManager = new MqttManager(MqttClientConnectionFn.apply,
        sourcesToConvMap,
        settings)
      Thread.sleep(2000)

      // The broker "crashes" and loses all state. The mqttManager should reconnect and resubscribe.
      stopMockMqttBroker()
      Thread.sleep(5000)

      // A new broker starts up. The MqttManager should now reconnect and resubscribe.
      startNewMockMqttBroker()
      Thread.sleep(5000)

      // Publish a message to the topic the manager should have resubscribed to.
      val message = "message1"
      publishMessage(source, message.getBytes())
      Thread.sleep(2000)

      val records = new util.LinkedList[SourceRecord]()
      mqttManager.getRecords(records)

      // Verify that the records were received by the manager.
      try {
        records.size() shouldBe 1
        records.get(0).topic() shouldBe target
        records.get(0).value() shouldBe message.getBytes()
        records.get(0).valueSchema() shouldBe Schema.BYTES_SCHEMA
      }
      finally {
        mqttManager.close()
      }
    }
  }

  "MqttManager" should {
    "process the messages on shared mqtt topic and create source records with Bytes schema with Wildcards" in {
      val sourceTopic = "$share/connect/mqttSourceTopic"
      val sourceKCQL = "`$share/connect/mqttSourceTopic`" // same with sourceTopic but with quotes
      val target = "kafkaTopic"
      val sourcesToConvMap = Map(sourceTopic -> new BytesConverter)
      val props = Map(
        MqttConfigConstants.CLEAN_SESSION_CONFIG -> "true",
        MqttConfigConstants.CONNECTION_TIMEOUT_CONFIG -> connectionTimeout.toString,
        MqttConfigConstants.KCQL_CONFIG -> s"INSERT INTO $target SELECT * FROM $sourceKCQL withregex=`\\$$share/connect/.*`",
        MqttConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG -> keepAlive.toString,
        MqttConfigConstants.CLIENT_ID_CONFIG -> clientId,
        MqttConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
        MqttConfigConstants.HOSTS_CONFIG -> connection,
        MqttConfigConstants.QS_CONFIG -> qs.toString
      )
      val mqttManager = new MqttManager(MqttClientConnectionFn.apply,
        sourcesToConvMap,
        MqttSourceSettings(MqttSourceConfig(props.asJava)))
      Thread.sleep(2000)

      val message = "message"

      publishMessage(sourceTopic, message.getBytes)

      Thread.sleep(2000)

      var records = new util.LinkedList[SourceRecord]()
      mqttManager.getRecords(records)

      try {
        records.size() shouldBe 1
        records.get(0).topic() shouldBe target
        records.get(0).value() shouldBe message.getBytes()
        records.get(0).valueSchema() shouldBe Schema.BYTES_SCHEMA
      }
      finally {
        mqttManager.close()
      }
    }

  }

  private def publishMessage(topic: String, payload: Array[Byte]) = {
    val message = new PublishMessage()
    message.setTopicName(s"$topic")
    message.setRetainFlag(false)
    message.setQos(AbstractMessage.QOSType.EXACTLY_ONCE)
    message.setPayload(ByteBuffer.wrap(payload))
    mqttBroker.foreach(_.internalPublish(message))

  }

}


case class Student(name: String, age: Int, note: Double)
