package com.datamountaineer.streamreactor.connect.mqtt.source.converters

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.ByteBuffer
import java.nio.file.Paths
import java.util.UUID

import com.sksamuel.avro4s._
import io.confluent.connect.avro.AvroData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.{AvroRuntimeException, Schema, SchemaBuilder}
import org.apache.kafka.common.record.ByteBufferOutputStream
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

class AvroConverterTest extends WordSpec with Matchers with BeforeAndAfterAll {
  private val topic = "topicA"
  private val mqttSource = "somesource"
  private val folder = new File(UUID.randomUUID().toString)
  folder.mkdir()

  override def beforeAll() = {

  }

  override def afterAll() = {
    folder.delete()
  }

  private def initializeConverter(converter: AvroConverter, schema: Schema) = {
    def writeSchema(schema: Schema): File = {
      val schemaFile = Paths.get(folder.getName, UUID.randomUUID().toString)
      val bw = new BufferedWriter(new FileWriter(schemaFile.toFile))
      bw.write(schema.toString)
      bw.close()

      schemaFile.toFile
    }

    converter.initialize(Map(
      AvroConverter.SCHEMA_CONFIG -> s"$mqttSource->${writeSchema(schema)}"
    ))

  }

  private def write(record: GenericRecord): Array[Byte] = {
    val byteBuffer = ByteBuffer.wrap(new Array(128))
    val writer = new SpecificDatumWriter[GenericRecord](record.getSchema)
    val encoder = EncoderFactory.get().directBinaryEncoder(new ByteBufferOutputStream(byteBuffer), null)

    writer.write(record, encoder)

    byteBuffer.flip()
    byteBuffer.array()
  }


  "AvroConverter" should {
    "handle null payloads" in {
      val converter = new AvroConverter()
      val schema = SchemaBuilder.builder().stringType()
      initializeConverter(converter, schema)

      val sourceRecord = converter.convert(topic, mqttSource, 100, null)

      sourceRecord.key() shouldBe null
      sourceRecord.keySchema() shouldBe null
      sourceRecord.value() shouldBe null
    }

    "throw an exception if it can't parse the payload" in {
      intercept[AvroRuntimeException] {
        val recordFormat = RecordFormat[Transaction]
        val transaction = Transaction("test", 2354.99, System.currentTimeMillis())
        val avro = recordFormat.to(transaction)

        val converter = new AvroConverter
        initializeConverter(converter, avro.getSchema)

        val sourceRecord = converter.convert(topic, mqttSource, 1001, write(avro).map(b => (b + 1) % 255).map(_.toByte))

        sourceRecord.key() shouldBe null
        sourceRecord.keySchema() shouldBe null

        val avroData = new AvroData(4)

        sourceRecord.value() shouldBe avroData.toConnectData(avro.getSchema, avro).value()

        sourceRecord.valueSchema() shouldBe avroData.toConnectSchema(avro.getSchema)
      }
    }

    "handle avro records" in {
      val recordFormat = RecordFormat[Transaction]
      val transaction = Transaction("test", 2354.99, System.currentTimeMillis())
      val avro = recordFormat.to(transaction)

      val converter = new AvroConverter
      initializeConverter(converter, avro.getSchema)

      val sourceRecord = converter.convert(topic, mqttSource, 1001, write(avro))

      sourceRecord.key() shouldBe null
      sourceRecord.keySchema() shouldBe null

      val avroData = new AvroData(4)
      sourceRecord.valueSchema() shouldBe avroData.toConnectSchema(avro.getSchema)

      sourceRecord.value() shouldBe avroData.toConnectData(avro.getSchema, avro).value()
    }
  }
}


case class Transaction(id: String, amount: Double, timestamp: Long)
