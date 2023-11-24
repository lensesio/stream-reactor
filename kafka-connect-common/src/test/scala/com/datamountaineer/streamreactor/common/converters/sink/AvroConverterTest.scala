/*
 * Copyright 2017-2023 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
///*
// *  Copyright 2017 Datamountaineer.
// *
// *  Licensed under the Apache License, Version 2.0 (the "License");
// *  you may not use this file except in compliance with the License.
// *  You may obtain a copy of the License at
// *
// *  http://www.apache.org/licenses/LICENSE-2.0
// *
// *  Unless required by applicable law or agreed to in writing, software
// *  distributed under the License is distributed on an "AS IS" BASIS,
// *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *  See the License for the specific language governing permissions and
// *  limitations under the License.
// */
//
//package io.lenses.streamreactor.common.converters.sink
//
//import io.lenses.streamreactor.common.converters.MsgKey
//import io.lenses.streamreactor.common.converters.sink.AvroConverter
//
//import java.io.{BufferedWriter, File, FileWriter}
//import java.nio.file.Paths
//import java.util.UUID
//import com.sksamuel.avro4s.RecordFormat
//import io.confluent.connect.avro.AvroData
//import org.apache.avro.{Schema, SchemaBuilder}
//import org.apache.kafka.connect.data.{Struct, Schema => KafkaSchema, SchemaBuilder => KafkaSchemaBuilder}
//import org.apache.kafka.connect.errors.DataException
//import org.apache.kafka.connect.sink.SinkRecord
//import org.scalatest.wordspec.AnyWordSpec
//import org.scalatest.BeforeAndAfterAll
//import org.scalatest.matchers.should.Matchers
//
//import scala.reflect.io.Path
//
//class AvroConverterTest extends AnyWordSpec with Matchers with BeforeAndAfterAll {
//  private val sinkTopic = "somesink"
//  private val folder = new File(UUID.randomUUID().toString)
//  folder.mkdir()
//  val path = Path(folder.getAbsolutePath)
//
//  val kafkaSchema = KafkaSchemaBuilder.struct.name("record")
//      .version(1)
//      .field("id", KafkaSchema.STRING_SCHEMA)
//      .field("amount", KafkaSchema.FLOAT64_SCHEMA)
//      .field("timestamp", KafkaSchema.INT64_SCHEMA)
//      .build
//
//  override def beforeAll() = {
//
//  }
//
//  override def afterAll() = {
//    path.deleteRecursively()
//  }
//
//  private def initializeConverter(converter: AvroConverter, schema: Schema) = {
//    def writeSchema(schema: Schema): File = {
//      val schemaFile = Paths.get(folder.getName, UUID.randomUUID().toString)
//      val bw = new BufferedWriter(new FileWriter(schemaFile.toFile))
//      bw.write(schema.toString)
//      bw.close()
//
//      schemaFile.toFile
//    }
//
//    converter.initialize(Map(
//      AvroConverter.SCHEMA_CONFIG -> s"$sinkTopic=${writeSchema(schema)}"
//    ))
//
//  }
//
//  "Sink AvroConverter" should {
//    "handle null payloads" in {
//      val converter = new AvroConverter()
//      val schema = SchemaBuilder.builder().stringType()
//      initializeConverter(converter, schema)
//
//      val sinkRecord = converter.convert(sinkTopic, null)
//
//      sinkRecord.key() shouldBe null
//      sinkRecord.keySchema() shouldBe null
//      sinkRecord.value() shouldBe null
//    }
//
//    "throw an exception if the payload contains a wrong type" in {
//      intercept[DataException] {
//        val recordFormat = RecordFormat[Transaction]
//        val transaction = Transaction("test", 2354.99, System.currentTimeMillis())
//        val avro = recordFormat.to(transaction)
//
//        val converter = new AvroConverter
//        initializeConverter(converter, avro.getSchema)
//
//        val payload = new Struct(kafkaSchema)
//            .put("id", 15)
//            .put("amount", 2354.99)
//            .put("timestamp", 1578467749572L)
//        val data = new SinkRecord(sinkTopic, 0, null, "keyA", kafkaSchema, payload, 0)
//        val sinkRecord = converter.convert(sinkTopic, data)
//      }
//    }
//
//    "create avro records" in {
//      val recordFormat = RecordFormat[Transaction]
//      val transaction = Transaction("test", 2354.99, 1578467749572L)
//      val avro = recordFormat.to(transaction)
//
//      val converter = new AvroConverter
//      initializeConverter(converter, avro.getSchema)
//
//      val payload = new Struct(kafkaSchema)
//        .put("id", "test")
//        .put("amount", 2354.99)
//        .put("timestamp", 1578467749572L)
//      val data = new SinkRecord(sinkTopic, 0, null, "keyA", kafkaSchema, payload, 0)
//      val sinkRecord = converter.convert(sinkTopic, data)
//
//      sinkRecord.key() shouldBe MsgKey.getStruct(sinkTopic, "keyA")
//      sinkRecord.keySchema() shouldBe MsgKey.schema
//
//      val avroData = new AvroData(4)
//      sinkRecord.valueSchema() shouldBe kafkaSchema
//
//      sinkRecord.value() shouldBe Array(8, 116, 101, 115, 116, 20, -82, 71, -31,
//                                        -6, 101, -94, 64, -120, -69, -109, -64,
//                                        -16, 91).map(_.toByte)
//    }
//  }
//}
//
//
//case class Transaction(id: String, amount: Double, timestamp: Long)
