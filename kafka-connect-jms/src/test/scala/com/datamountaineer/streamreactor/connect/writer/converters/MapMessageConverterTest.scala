package com.datamountaineer.streamreactor.connect.writer.converters

import javax.jms.MapMessage

import com.datamountaineer.streamreactor.connect.jms.sink.writer.converters.MapMessageConverter
import com.sksamuel.scalax.io.Using
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.broker.BrokerService
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.collection.JavaConverters._

class MapMessageConverterTest extends WordSpec with Matchers with Using with BeforeAndAfter {
  val converter = new MapMessageConverter()


  "MapMessageConverter" should {
    "create a JMS MapMessage" in {
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
          val msg = converter.convert(record, session).asInstanceOf[MapMessage]

          Option(msg).isDefined shouldBe true

          msg.getBoolean("boolean") shouldBe true
          msg.getByte("int8") shouldBe 12.toByte
          msg.getShort("int16") shouldBe 12.toShort
          msg.getInt("int32") shouldBe 12
          msg.getLong("int64") shouldBe 12L
          msg.getFloat("float32") shouldBe 12.2f
          msg.getDouble("float64") shouldBe 12.2
          msg.getString("string") shouldBe "foo"
          msg.getBytes("bytes") shouldBe "foo".getBytes()
          val arr = msg.getObject("array")
          arr.asInstanceOf[java.util.List[String]].asScala.toArray shouldBe Array("a", "b", "c")

          val map1 = msg.getObject("map").asInstanceOf[java.util.Map[String, Int]].asScala.toMap
          map1 shouldBe Map("field" -> 1)

          val map2 = msg.getObject("mapNonStringKeys").asInstanceOf[java.util.Map[Int, Int]].asScala.toMap
          map2 shouldBe Map(1 -> 1)

        }
      }
    }
  }
}
