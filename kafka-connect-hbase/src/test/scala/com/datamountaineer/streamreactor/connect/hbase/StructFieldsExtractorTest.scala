package com.datamountaineer.streamreactor.connect.hbase

import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.scalatest.{Matchers, WordSpec}


class StructFieldsExtractorTest extends WordSpec with Matchers {
  "StructFieldsExtractor" should {
    "return all the fields and their bytes value" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct = new Struct(schema)
        .put("firstName", "Alex")
        .put("lastName", "Smith")
        .put("age", 30)

      val map = new StructFieldsExtractor(true, Map.empty).get(struct).toMap

      Bytes.toString(map.get("firstName").get) shouldBe "Alex"
      Bytes.toString(map.get("lastName").get) shouldBe "Smith"
      Bytes.toInt(map.get("age").get) shouldBe 30

    }

    "return all fields and apply the mapping" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct = new Struct(schema)
        .put("firstName", "Alex")
        .put("lastName", "Smith")
        .put("age", 30)

      val map = new StructFieldsExtractor(true, Map("lastName" -> "Name", "age" -> "a")).get(struct).toMap

      Bytes.toString(map.get("firstName").get) shouldBe "Alex"
      Bytes.toString(map.get("Name").get) shouldBe "Smith"
      Bytes.toInt(map.get("a").get) shouldBe 30

    }

    "return only the specified fields" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct = new Struct(schema)
        .put("firstName", "Alex")
        .put("lastName", "Smith")
        .put("age", 30)

      val map = new StructFieldsExtractor(false, Map("lastName" -> "Name", "age" -> "age")).get(struct).toMap

      Bytes.toString(map.get("Name").get) shouldBe "Smith"
      Bytes.toInt(map.get("age").get) shouldBe 30

      map.size shouldBe 2
    }
  }

}
