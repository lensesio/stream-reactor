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

import com.datamountaineer.streamreactor.common.converters.MsgKey
import com.datamountaineer.streamreactor.common.serialization.AvroSerializer
import com.datamountaineer.streamreactor.connect.converters.source.{AvroConverter, BytesConverter, JsonSimpleConverter}
import com.datamountaineer.streamreactor.connect.mqtt.config.MqttConfigConstants
import com.sksamuel.avro4s.SchemaFor
import org.apache.kafka.connect.data.{Schema, Struct}
import org.apache.kafka.connect.source.SourceTaskContext
import org.eclipse.paho.client.mqttv3.{MqttClient, MqttMessage}
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.testcontainers.containers.GenericContainer

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.Paths
import java.util.UUID
import scala.collection.JavaConverters._

class MqttSourceTaskTest extends AnyWordSpec with Matchers with BeforeAndAfter with MockitoSugar {

  val mqttContainer : GenericContainer[_] = new GenericContainer("eclipse-mosquitto")
    .withExposedPorts(1883)

  before {
    mqttContainer.start()
  }

  after {
    mqttContainer.stop()
  }

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


    val studentSchema = SchemaFor[Student]()
    val task = new MqttSourceTask

    val connection = s"tcp://0.0.0.0:${mqttContainer.getMappedPort(1883)}"
    val props = Map(
      MqttConfigConstants.CLEAN_SESSION_CONFIG -> "true",
      MqttConfigConstants.CONNECTION_TIMEOUT_CONFIG -> "1000",
      MqttConfigConstants.KCQL_CONFIG -> s"INSERT INTO $target1 SELECT * FROM $source1 WITHCONVERTER=`${classOf[BytesConverter].getCanonicalName}`;INSERT INTO $target2 SELECT * FROM $source2 WITHCONVERTER=`${classOf[JsonSimpleConverter].getCanonicalName}`;INSERT INTO $target3 SELECT * FROM $source3 WITHCONVERTER=`${classOf[AvroConverter].getCanonicalName}`",
      MqttConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG -> "1000",
      AvroConverter.SCHEMA_CONFIG -> s"$source3=${getSchemaFile(studentSchema)}",
      MqttConfigConstants.CLIENT_ID_CONFIG -> "MqttSourceTaskTest",
      MqttConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
      MqttConfigConstants.HOSTS_CONFIG -> connection,
      MqttConfigConstants.QS_CONFIG -> "1"
    ).asJava
    val context = mock[SourceTaskContext]
    when(context.configs()).thenReturn(props)
    task.initialize(context)
    task.start(props)
    Thread.sleep(2000)


    val message1 = "message1".getBytes()
    val student = Student("Mike Bush", 19, 9.3)
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
          record.valueSchema().field("age").schema().`type`()shouldBe Schema.INT32_SCHEMA.`type`()
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

    val connection = s"tcp://0.0.0.0:${mqttContainer.getMappedPort(1883)}"
    val client = new MqttClient(connection, UUID.randomUUID.toString)
    client.connect()
    client.publish(topic, msg)
    client.disconnect()
    client.close()
  }
}
