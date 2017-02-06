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

import javax.jms.ObjectMessage

import com.datamountaineer.streamreactor.connect.jms.sink.writer.converters.ObjectMessageConverter
import com.sksamuel.scalax.io.Using
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class ObjectMessageConverterTest extends WordSpec with Matchers with Using {
  val converter = new ObjectMessageConverter()

  "ObjectMessageConverter" should {
    "create an instance of jms ObjectMessage" in {
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
          val msg = converter.convert(record, session).asInstanceOf[ObjectMessage]

          Option(msg).isDefined shouldBe true

          msg.getBooleanProperty("boolean") shouldBe true
          msg.getByteProperty("int8") shouldBe 12.toByte
          msg.getShortProperty("int16") shouldBe 12.toShort
          msg.getIntProperty("int32") shouldBe 12
          msg.getLongProperty("int64") shouldBe 12L
          msg.getFloatProperty("float32") shouldBe 12.2f
          msg.getDoubleProperty("float64") shouldBe 12.2
          msg.getStringProperty("string") shouldBe "foo"
          msg.getObjectProperty("bytes").asInstanceOf[java.util.List[Byte]].toArray shouldBe "foo".getBytes()
          val arr = msg.getObjectProperty("array")
          arr.asInstanceOf[java.util.List[String]].asScala.toArray shouldBe Array("a", "b", "c")

          val map1 = msg.getObjectProperty("map").asInstanceOf[java.util.Map[String, Int]].asScala.toMap
          map1 shouldBe Map("field" -> 1)

          val map2 = msg.getObjectProperty("mapNonStringKeys").asInstanceOf[java.util.Map[Int, Int]].asScala.toMap
          map2 shouldBe Map(1 -> 1)
        }
      }
    }
  }
}
