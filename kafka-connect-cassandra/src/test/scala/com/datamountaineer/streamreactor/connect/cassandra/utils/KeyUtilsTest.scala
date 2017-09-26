package com.datamountaineer.streamreactor.connect.cassandra.utils

import java.util.UUID

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.scalatest.{Matchers, WordSpec}

class KeyUtilsTest extends WordSpec with Matchers {

  val uuid = UUID.randomUUID()
  val key1 = Int.box(uuid.hashCode)
  val key2 = uuid.toString

  "KeyUtilsTest.keysFromJson" should {

    val keySchema = SchemaBuilder.string().build

    "return key values from flat json structure" in {

      val flatKey =
        s"""
           | {
           |   \"key1\": $key1,
           |   \"key2\": \"$key2\"
           | }
       """.stripMargin

      KeyUtils.keysFromJson(flatKey, Seq("key1", "key2")) shouldEqual Seq(key1, key2)
    }

    "return key values from complex json structure" in {

      val complexKey =
        s"""
           | {
           |   \"key1\": $key1,
           |   \"nested\": {
           |     \"key2\": \"$key2\"
           |   }
           | }
       """.stripMargin

      KeyUtils.keysFromJson(complexKey, Seq("key1", "nested.key2")) shouldEqual Seq(key1, key2)
    }

    "throw exception when value not found" in {
      val flatKey =
        s"""
           | {
           |   \"key1\": $key1,
           |   \"key2\": \"$key2\"
           | }
       """.stripMargin
      assertThrows[Exception] { // TODO can this be more specific?
        KeyUtils.keysFromJson(flatKey, Seq("not-a-field"))
      }
    }
  }

  "KeyUtilsTest.keysFromStruct" should {

    "return key values from flat Struct object" in {
      val keySchema = SchemaBuilder.struct
        .field("key1", Schema.INT32_SCHEMA)
        .field("key2", Schema.STRING_SCHEMA)
        .build
      val keyStruct = new Struct(keySchema)
      keyStruct.put("key1", key1)
      keyStruct.put("key2", key2)

      KeyUtils.keysFromStruct(keyStruct, keySchema, Seq("key1", "key2")) shouldEqual Seq(key1, key2)
    }

    "return key values from complex Struct object" in {
      val nestedSchema = SchemaBuilder.struct
        .field("inside", Schema.STRING_SCHEMA)
        .build()

      val keySchema = SchemaBuilder.struct
        .field("key1", Schema.INT32_SCHEMA)
        .field("nested", nestedSchema)
        .build

      val nested = new Struct(nestedSchema)
      nested.put("inside", key2)

      val keyStruct = new Struct(keySchema)
      keyStruct.put("key1", key1)
      keyStruct.put("nested", nested)

      KeyUtils.keysFromStruct(keyStruct, keySchema, Seq("key1", "nested.inside")) shouldEqual Seq(key1, key2)
    }
  }
}
