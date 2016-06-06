package com.datamountaineer.streamreactor.connect.hbase

import com.datamountaineer.streamreactor.connect.rowkeys.{StructFieldsRowKeyBuilderBytes}
import com.datamountaineer.streamreactor.connect.schemas.BytesHelper._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{Matchers, WordSpec}

class StructFieldsRowKeyBuilderTest extends WordSpec with Matchers {
  "StructFieldsRowKeyBuilder" should {
    "raise an exception if the field is not present in the struct" in {
      intercept[IllegalArgumentException] {
        val schema = SchemaBuilder.struct().name("com.example.Person")
          .field("firstName", Schema.STRING_SCHEMA)
          .field("age", Schema.INT32_SCHEMA)
          .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

        val struct = new Struct(schema).put("firstName", "Alex").put("age", 30)

        val sinkRecord = new SinkRecord("sometopic", 1, null, null, schema, struct, 1)
        //val field = Field("threshold", "threshold", false)

        StructFieldsRowKeyBuilderBytes(List("threshold")).build(sinkRecord, null)
      }
    }

    "create the row key based on one single field in the struct" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct = new Struct(schema).put("firstName", "Alex").put("age", 30)

      //val field = Field("firstName", "firstName", true)
      val sinkRecord = new SinkRecord("sometopic", 1, null, null, schema, struct, 1)
      StructFieldsRowKeyBuilderBytes(List("firstName")).build(sinkRecord, null) shouldBe "Alex".fromString
    }

    "create the row key based on more thant one field in the struct" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct = new Struct(schema).put("firstName", "Alex").put("age", 30)

      //val field = Field("firstName", "firstName", true)
      //val field2 = Field("age", "age", true)
      val sinkRecord = new SinkRecord("sometopic", 1, null, null, schema, struct, 1)
      StructFieldsRowKeyBuilderBytes(List("firstName", "age")).build(sinkRecord, null) shouldBe
        Bytes.add("Alex".fromString(), "\n".fromString(), 30.fromInt())
    }
  }
}
