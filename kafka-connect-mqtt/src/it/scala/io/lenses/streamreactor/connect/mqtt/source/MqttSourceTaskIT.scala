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

package io.lenses.streamreactor.connect.mqtt.source

import io.lenses.streamreactor.common.converters.MsgKey
import io.lenses.streamreactor.common.serialization.AvroSerializer
import io.lenses.streamreactor.connect.converters.source.AvroConverter
import io.lenses.streamreactor.connect.converters.source.BytesConverter
import io.lenses.streamreactor.connect.converters.source.JsonSimpleConverter
import io.lenses.streamreactor.connect.mqtt.config.MqttConfigConstants
import com.dimafeng.testcontainers.ForAllTestContainer
import com.dimafeng.testcontainers.GenericContainer
import com.sksamuel.avro4s.AvroSchema
import com.sksamuel.avro4s.RecordFormat
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceTaskContext
import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.nio.file.Paths
import java.util.UUID
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava

class MqttSourceTaskIT extends AnyWordSpec with ForAllTestContainer with Matchers with MockitoSugar with StrictLogging {

  private val mqttPort = 1883

  override val container = GenericContainer("eclipse-mosquitto:1.4.12", exposedPorts = Seq(mqttPort))

  protected def getMqttConnectionUrl = s"tcp://${container.host}:${container.mappedPort(mqttPort)}"

  private def getSchemaFile(schema: org.apache.avro.Schema) = {
    val schemaFile = Paths.get(UUID.randomUUID().toString)

    def writeSchema(schema: org.apache.avro.Schema): File = {

      val bw = new BufferedWriter(new FileWriter(schemaFile.toFile))
      bw.write(schema.toString)
      bw.close()

      val file = schemaFile.toFile
      file.deleteOnExit()
      file
    }

    writeSchema(schema)
  }

  "should start a task and subscribe to the topics provided" in {

    val source1 = "/mqttSource1"
    val source2 = "/mqttSource2"
    val source3 = "/mqttSource3"

    val target1 = "kafkaTopic1"
    val target2 = "kafkaTopic2"
    val target3 = "kafkaTopic3"

    implicit val studentSchema = AvroSchema[Student]
    implicit val recordFormat  = RecordFormat[Student]

    val task = new MqttSourceTask

    val props = Map(
      MqttConfigConstants.CLEAN_SESSION_CONFIG      -> "true",
      MqttConfigConstants.CONNECTION_TIMEOUT_CONFIG -> "1000",
      MqttConfigConstants.KCQL_CONFIG -> s"INSERT INTO $target1 SELECT * FROM $source1 WITHCONVERTER=`${classOf[
        BytesConverter,
      ].getCanonicalName}`;INSERT INTO $target2 SELECT * FROM $source2 WITHCONVERTER=`${classOf[
        JsonSimpleConverter,
      ].getCanonicalName}`;INSERT INTO $target3 SELECT * FROM $source3 WITHCONVERTER=`${classOf[AvroConverter].getCanonicalName}`",
      MqttConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG     -> "1000",
      AvroConverter.SCHEMA_CONFIG                        -> s"$source3=${getSchemaFile(studentSchema)}",
      MqttConfigConstants.CLIENT_ID_CONFIG               -> UUID.randomUUID().toString,
      MqttConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
      MqttConfigConstants.HOSTS_CONFIG                   -> getMqttConnectionUrl,
      MqttConfigConstants.QS_CONFIG                      -> "1",
    ).asJava
    val context = mock[SourceTaskContext]
    when(context.configs()).thenReturn(props)
    task.initialize(context)
    task.start(props)
    Thread.sleep(2000)

    val message1 = "message1".getBytes()
    val student  = Student("Mike Bush", 19, 9.3)
    val message2 = JacksonJson.toJson(student).getBytes
    val message3 = AvroSerializer.getBytes(student)

    publishMessage(source1, message1)
    publishMessage(source2, message2)
    publishMessage(source3, message3)
    Thread.sleep(2000)

    val records = task.poll().asScala
    records.size shouldBe 3

    records.foreach { record =>
      record.keySchema() shouldBe MsgKey.schema
      val source = record.key().asInstanceOf[Struct].get("topic")

      record.topic() match {
        case `target1` =>
          source shouldBe source1
          record.valueSchema() shouldBe Schema.BYTES_SCHEMA
          record.value() shouldBe message1

        case `target2` =>
          source shouldBe source2

          record.valueSchema().field("name").schema().`type`() shouldBe Schema.STRING_SCHEMA.`type`()
          record.valueSchema().field("name").index() shouldBe 0
          record.valueSchema().field("age").schema().`type`() shouldBe Schema.INT64_SCHEMA.`type`()
          record.valueSchema().field("age").index() shouldBe 1
          record.valueSchema().field("note").schema().`type`() shouldBe Schema.FLOAT64_SCHEMA.`type`()
          record.valueSchema().field("note").index() shouldBe 2

          val struct = record.value().asInstanceOf[Struct]
          struct.getString("name") shouldBe student.name
          struct.getInt64("age") shouldBe student.age.toLong
          struct.getFloat64("note") shouldBe student.note

        case `target3` =>
          source shouldBe source3

          record.valueSchema().field("name").schema().`type`() shouldBe Schema.STRING_SCHEMA.`type`()
          record.valueSchema().field("name").index() shouldBe 0
          record.valueSchema().field("age").schema().`type`() shouldBe Schema.INT32_SCHEMA.`type`()
          record.valueSchema().field("age").index() shouldBe 1
          record.valueSchema().field("note").schema().`type`() shouldBe Schema.FLOAT64_SCHEMA.`type`()
          record.valueSchema().field("note").index() shouldBe 2

          val struct = record.value().asInstanceOf[Struct]
          struct.getString("name") shouldBe student.name
          struct.getInt32("age") shouldBe student.age
          struct.getFloat64("note") shouldBe student.note

        case other => fail(s"$other is not a valid topic")
      }

    }

    task.stop()
  }

  private def publishMessage(topic: String, payload: Array[Byte]): Unit = {
    val msg = new MqttMessage(payload)
    msg.setQos(2)
    msg.setRetained(false)

    val client = new MqttClient(getMqttConnectionUrl, UUID.randomUUID.toString)
    client.connect()
    client.publish(topic, msg)
    client.disconnect()
    client.close()
  }
}
