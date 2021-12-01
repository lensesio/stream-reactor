package com.datamountaineer.streamreactor.connect.cassandra.utils

import java.util.UUID

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class KeyUtilsTest extends AnyWordSpec with Matchers {

  val uuid = UUID.randomUUID()
  val key1 = Int.box(uuid.hashCode)
  val key2 = uuid.toString
  val key3 = Long.box(uuid.hashCode().toLong)

  "KeyUtilsTest.keysFromJson" should {

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
      val nestedSecondLevelSchema = SchemaBuilder.struct
        .field("insideOfNested", Schema.INT64_SCHEMA)
        .build()

      val nestedSchema = SchemaBuilder.struct
        .field("inside", Schema.STRING_SCHEMA)
        .field("nestedOfNested", nestedSecondLevelSchema)
        .build()

      val keySchema = SchemaBuilder.struct
        .field("key1", Schema.INT32_SCHEMA)
        .field("nested", nestedSchema)
        .build

      val nestedSecondLevel = new Struct(nestedSecondLevelSchema)
      nestedSecondLevel.put("insideOfNested", key3)

      val nested = new Struct(nestedSchema)
      nested.put("inside", key2)
      nested.put("nestedOfNested", nestedSecondLevel)

      val keyStruct = new Struct(keySchema)
      keyStruct.put("key1", key1)
      keyStruct.put("nested", nested)

      KeyUtils.keysFromStruct(keyStruct, keySchema, Seq("key1", "nested.inside", "nested.nestedOfNested.insideOfNested")) shouldEqual Seq(key1, key2, key3)
    }

    "return key values in correct key types for obligatory schema" in {
      val keySchema = SchemaBuilder.struct
        .field("int8", Schema.INT8_SCHEMA)
        .field("int16", Schema.INT16_SCHEMA)
        .field("int32", Schema.INT32_SCHEMA)
        .field("int64", Schema.INT64_SCHEMA)
        .field("float32", Schema.FLOAT32_SCHEMA)
        .field("float64", Schema.FLOAT64_SCHEMA)
        .field("bool", Schema.BOOLEAN_SCHEMA)
        .field("string", Schema.STRING_SCHEMA)
        .field("bytes", Schema.BYTES_SCHEMA)

      val keyStruct = new Struct(keySchema)
      keyStruct.put("int8", 0.asInstanceOf[Byte])
      keyStruct.put("int16", 0.toShort)
      keyStruct.put("int32", 0.toInt)
      keyStruct.put("int64", 0.toLong)
      keyStruct.put("float32", 0.toFloat)
      keyStruct.put("float64", 0.toDouble)
      keyStruct.put("bool", true)
      keyStruct.put("string", "some string")
      keyStruct.put("bytes", "bytes".getBytes)

      val fieldNames = Seq("int8", "int16", "int32", "int64", "float32", "float64", "bool", "string", "bytes")
      val objects = KeyUtils.keysFromStruct(keyStruct, keySchema, fieldNames)
      objects(0) shouldBe a [java.lang.Byte]
      objects(1) shouldBe a [java.lang.Short]
      objects(2) shouldBe a [java.lang.Integer]
      objects(3) shouldBe a [java.lang.Long]
      objects(4) shouldBe a [java.lang.Float]
      objects(5) shouldBe a [java.lang.Double]
      objects(6) shouldBe a [java.lang.Boolean]
      objects(7) shouldBe a [java.lang.String]
      objects(8).getClass.isArray shouldBe true
      objects(8).getClass.getComponentType.getCanonicalName shouldBe "byte"
    }
  }
}
