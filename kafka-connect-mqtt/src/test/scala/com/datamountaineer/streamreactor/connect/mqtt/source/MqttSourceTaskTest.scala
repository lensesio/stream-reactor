package com.datamountaineer.streamreactor.connect.mqtt.source

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.ByteBuffer
import java.nio.file.Paths
import java.util.UUID

import com.datamountaineer.streamreactor.connect.converters.source.{AvroConverter, BytesConverter, JsonSimpleConverter, MsgKey}
import com.datamountaineer.streamreactor.connect.mqtt.config.MqttSourceConfig
import com.datamountaineer.streamreactor.connect.serialization.AvroSerializer
import com.sksamuel.avro4s.{RecordFormat, SchemaFor}
import io.confluent.connect.avro.AvroData
import io.moquette.proto.messages.{AbstractMessage, PublishMessage}
import io.moquette.server.Server
import io.moquette.server.config.ClasspathConfig
import org.apache.kafka.connect.data.{Schema, Struct}
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.collection.JavaConversions._

class MqttSourceTaskTest extends WordSpec with Matchers with BeforeAndAfter {
  val classPathConfig = new ClasspathConfig()

  val connection = "tcp://0.0.0.0:1883"
  val clientId = "MqttSourceTaskTest"
  val qs = 1
  val connectionTimeout = 1000
  val keepAlive = 1000

  var mqttBroker: Option[Server] = None
  before {
    mqttBroker = Some(new Server())
    mqttBroker.foreach(_.startServer(classPathConfig))
  }

  after {
    mqttBroker.foreach {
      _.stopServer()
    }
  }

  private def getSchemaFile(mqttSource: String, schema: org.apache.avro.Schema) = {
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
    task.start(Map(
      MqttSourceConfig.CLEAN_SESSION_CONFIG -> "true",
      MqttSourceConfig.CONNECTION_TIMEOUT_CONFIG -> connectionTimeout.toString,
      MqttSourceConfig.KCQL_CONFIG -> s"INSERT INTO $target1 SELECT * FROM $source1;INSERT INTO $target2 SELECT * FROM $source2;INSERT INTO $target3 SELECT * FROM $source3",
      MqttSourceConfig.KEEP_ALIVE_INTERVAL_CONFIG -> keepAlive.toString,
      MqttSourceConfig.CONVERTER_CONFIG -> s"$source1=${classOf[BytesConverter].getCanonicalName};$source2=${classOf[JsonSimpleConverter].getCanonicalName};$source3=${classOf[AvroConverter].getCanonicalName};",
      AvroConverter.SCHEMA_CONFIG -> s"$source3=${getSchemaFile(source3, studentSchema)}",
      MqttSourceConfig.CLIENT_ID_CONFIG -> clientId,
      MqttSourceConfig.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
      MqttSourceConfig.HOSTS_CONFIG -> connection,
      MqttSourceConfig.QS_CONFIG -> qs.toString
    ))


    val message1 = "message1".getBytes()


    val student = Student("Mike Bush", 19, 9.3)
    val message2 = JacksonJson.toJson(student).getBytes

    val recordFormat = RecordFormat[Student]

    val message3 = AvroSerializer.getBytes(student)

    publishMessage(source1, message1)
    publishMessage(source2, message2)
    publishMessage(source3, message3)
    Thread.sleep(2000)

    val records = task.poll()
    records.size() shouldBe 3

    val avroData = new AvroData(4)

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

    task.stop()
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
