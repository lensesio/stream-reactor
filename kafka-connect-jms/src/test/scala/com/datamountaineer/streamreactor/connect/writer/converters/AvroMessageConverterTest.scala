/*
 *  Copyright 2017 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.writer.converters

import java.nio.ByteBuffer
import javax.jms.BytesMessage

import com.datamountaineer.streamreactor.connect.AvroDeserializer
import com.datamountaineer.streamreactor.connect.jms.sink.writer.converters.AvroMessageConverter
import com.sksamuel.scalax.io.Using
import io.confluent.connect.avro.AvroData
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class AvroMessageConverterTest extends WordSpec with Matchers with Using {
  val converter = new AvroMessageConverter()
  private lazy val avroData = new AvroData(128)

  "AvroMessageConverter" should {
    "create a BytesMessage with avro payload" in {
      val connectionFactory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false")

      using(connectionFactory.createConnection()) { connection =>
        using(connection.createSession(false, 1)) { session =>
          val builder = SchemaBuilder.struct
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
            .field("mapNonStringKeys", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA)
              .build())
          val schema = builder.build()
          val struct = new Struct(schema)
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

          val record = new SinkRecord("topic", 0, null, null, schema, struct, 1)
          val msg = converter.convert(record, session).asInstanceOf[BytesMessage]

          Option(msg).isDefined shouldBe true

          msg.reset()

          val size = msg.getBodyLength

          size > 0 shouldBe true
          val data = new Array[Byte](size.toInt)
          msg.readBytes(data)

          val avroRecord = AvroDeserializer(data, avroData.fromConnectSchema(schema))
          avroRecord.get("int8") shouldBe 12.toByte
          avroRecord.get("int16") shouldBe 12.toShort
          avroRecord.get("int32") shouldBe 12
          avroRecord.get("int64") shouldBe 12L
          avroRecord.get("float32") shouldBe 12.2f
          avroRecord.get("float64") shouldBe 12.2
          avroRecord.get("boolean") shouldBe true
          avroRecord.get("string").toString shouldBe "foo"
          avroRecord.get("bytes").asInstanceOf[ByteBuffer].array() shouldBe "foo".getBytes()
          val array = avroRecord.get("array").asInstanceOf[GenericData.Array[Utf8]]
          val iter = array.iterator()
          new Iterator[String] {
            override def hasNext: Boolean = iter.hasNext

            override def next(): String = iter.next().toString
          }.toSeq shouldBe Seq("a", "b", "c")
          val map = avroRecord.get("map").asInstanceOf[java.util.Map[Utf8, Int]].asScala
          map.size shouldBe 1
          map.keys.head.toString shouldBe "field"
          map.get(map.keys.head) shouldBe Some(1)

          val iterRecord = avroRecord.get("mapNonStringKeys").asInstanceOf[GenericData.Array[GenericData.Record]].iterator()
          iterRecord.hasNext shouldBe true
          val r = iterRecord.next()
          r.get("key") shouldBe 1
          r.get("value") shouldBe 1
        }
      }
    }
  }
}
