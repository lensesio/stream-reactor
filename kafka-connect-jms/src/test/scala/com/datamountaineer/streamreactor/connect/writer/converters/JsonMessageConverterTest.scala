package com.datamountaineer.streamreactor.connect.writer.converters

import javax.jms.TextMessage

import com.datamountaineer.streamreactor.connect.jms.sink.writer.converters.JsonMessageConverter
import com.sksamuel.scalax.io.Using
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.broker.BrokerService
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.collection.JavaConverters._


class JsonMessageConverterTest extends WordSpec with Matchers with Using {
  val converter = new JsonMessageConverter()

  "JsonMessageConverter" should {
    "create a TextMessage with Json payload" in {
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
          val msg = converter.convert(record, session).asInstanceOf[TextMessage]
          Option(msg).isDefined shouldBe true

          val json = msg.getText
          json shouldBe
            """{"int8":12,"int16":12,"int32":12,"int64":12,"float32":12.2,"float64":12.2,"boolean":true,"string":"foo","bytes":"Zm9v","array":["a","b","c"],"map":{"field":1},"mapNonStringKeys":[[1,1]]}""".stripMargin

        }
      }
    }
  }
}