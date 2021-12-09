package com.landoop.streamreactor.connect.hive

import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.kafka.connect.data.{Schema, SchemaBuilder}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class HiveSchemasTest extends AnyFlatSpec with Matchers {

  "toKafka" should "support string" in {
    HiveSchemas.toKafka("string", "myfield", true) shouldBe Schema.OPTIONAL_STRING_SCHEMA
    HiveSchemas.toKafka("string", "myfield", false) shouldBe Schema.STRING_SCHEMA
  }

  it should "support tinyint" in {
    HiveSchemas.toKafka("tinyint", "myfield", true) shouldBe Schema.OPTIONAL_INT8_SCHEMA
    HiveSchemas.toKafka("tinyint", "myfield", false) shouldBe Schema.INT8_SCHEMA
  }

  it should "support smallint" in {
    HiveSchemas.toKafka("smallint", "myfield", true) shouldBe Schema.OPTIONAL_INT16_SCHEMA
    HiveSchemas.toKafka("smallint", "myfield", false) shouldBe Schema.INT16_SCHEMA
  }

  it should "support bigint" in {
    HiveSchemas.toKafka("bigint", "myfield", true) shouldBe Schema.OPTIONAL_INT64_SCHEMA
    HiveSchemas.toKafka("bigint", "myfield", false) shouldBe Schema.INT64_SCHEMA
  }

  it should "support double" in {
    HiveSchemas.toKafka("double", "myfield", true) shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    HiveSchemas.toKafka("double", "myfield", false) shouldBe Schema.FLOAT64_SCHEMA
  }

  it should "support float" in {
    HiveSchemas.toKafka("float", "myfield", true) shouldBe Schema.OPTIONAL_FLOAT32_SCHEMA
    HiveSchemas.toKafka("float", "myfield", false) shouldBe Schema.FLOAT32_SCHEMA
  }

  it should "support boolean" in {
    HiveSchemas.toKafka("boolean", "myfield", true) shouldBe Schema.OPTIONAL_BOOLEAN_SCHEMA
    HiveSchemas.toKafka("boolean", "myfield", false) shouldBe Schema.BOOLEAN_SCHEMA
  }

  it should "support date" in {
    HiveSchemas.toKafka("date", "myfield", true) shouldBe Schema.OPTIONAL_INT64_SCHEMA
  }

  it should "support varchar" in {
    HiveSchemas.toKafka("varchar(3)", "myfield", true) shouldBe Schema.OPTIONAL_STRING_SCHEMA
    HiveSchemas.toKafka("varchar(  3  )", "myfield", true) shouldBe Schema.OPTIONAL_STRING_SCHEMA
  }

  it should "support char" in {
    HiveSchemas.toKafka("char(3)", "myfield", true) shouldBe Schema.OPTIONAL_STRING_SCHEMA
    HiveSchemas.toKafka("char(  3  )", "myfield", true) shouldBe Schema.OPTIONAL_STRING_SCHEMA
  }

  //  it should "support decimal" in {
  //    Schemas.toKafka("decimal(3,4)", "myfield", true) shouldBe DecimalOrNullDataType(Precision(3), Scale(4))
  //    Schemas.toKafka("decimal(  3,   4)", "myfield", true) shouldBe DecimalOrNullDataType(Precision(3), Scale(4))
  //    Schemas.toKafka("decimal(  3  ,   4  )", "myfield", true) shouldBe DecimalOrNullDataType(Precision(3), Scale(4))
  //    Schemas.toKafka("decimal(  3  ,4  )", "myfield", true) shouldBe DecimalOrNullDataType(Precision(3), Scale(4))
  //  }

  it should "support arrays" in {
    HiveSchemas.toKafka("array<string>", "myfield", true) shouldBe SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build()
    HiveSchemas.toKafka("array<    boolean    >", "myfield", true) shouldBe SchemaBuilder.array(Schema.OPTIONAL_BOOLEAN_SCHEMA).optional().build()
    HiveSchemas.toKafka("array<bigint    >", "myfield", true) shouldBe SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build()
    HiveSchemas.toKafka("array<   float>", "myfield", true) shouldBe SchemaBuilder.array(Schema.OPTIONAL_FLOAT32_SCHEMA).optional().build()
  }

  it should "support structs" in {
    val a = HiveSchemas.toKafka("struct<a:boolean, b:float>", "myfield", true)
    val b = SchemaBuilder.struct()
      .optional()
      //.name("myfield") // TODO: is this correct?
      .field("a", Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .field("b", Schema.OPTIONAL_FLOAT32_SCHEMA)
      .build()
    a shouldBe b
    HiveSchemas.toKafka("struct<  a   : boolean  , b   : float  >", "myfield", true)
    SchemaBuilder.struct()
      //.name("myfield")
      .field("a", Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .field("b", Schema.OPTIONAL_FLOAT32_SCHEMA)
      .build()
  }

  it should "support arrays in structs" in {
    val a = HiveSchemas.toKafka("struct< a:string, b:array<boolean>>", "myfield", true)
    val b = SchemaBuilder.struct()
      .optional()
      //.name("myfield")
      .field("a", Schema.OPTIONAL_STRING_SCHEMA)
      .field("b", SchemaBuilder.array(Schema.OPTIONAL_BOOLEAN_SCHEMA).optional().build())
      .build()
    a shouldBe b
  }

  it should "build from a complete table schema" in {
    val fields = Seq(
      new FieldSchema("a", "string", null),
      new FieldSchema("b", "boolean", null),
      new FieldSchema("c", "bigint", null),
      new FieldSchema("d", "double", null),
      new FieldSchema("e", "char(4)", null)
    )
    val parts = Seq(
      new FieldSchema("x", "string", null),
      new FieldSchema("y", "boolean", null)
    )
    val a = HiveSchemas.toKafka(fields, parts, "mystruct")
    val b = SchemaBuilder.struct()
      .optional()
      .name("mystruct")
      .field("a", Schema.OPTIONAL_STRING_SCHEMA)
      .field("b", Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .field("c", Schema.OPTIONAL_INT64_SCHEMA)
      .field("d", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("e", Schema.OPTIONAL_STRING_SCHEMA)
      .field("x", Schema.STRING_SCHEMA)
      .field("y", Schema.BOOLEAN_SCHEMA)
      .build()
    a shouldBe b
  }

  "to hive" should "support strings" in {
    HiveSchemas.toHiveType(Schema.STRING_SCHEMA) shouldBe "string"
  }

  it should "support longs" in {
    HiveSchemas.toHiveType(Schema.INT64_SCHEMA) shouldBe "bigint"
  }

  it should "support smallint" in {
    HiveSchemas.toHiveType(Schema.INT16_SCHEMA) shouldBe "smallint"
    HiveSchemas.toHiveType(Schema.OPTIONAL_INT16_SCHEMA) shouldBe "smallint"
  }

  it should "support tinyint" in {
    HiveSchemas.toHiveType(Schema.INT8_SCHEMA) shouldBe "tinyint"
    HiveSchemas.toHiveType(Schema.OPTIONAL_INT8_SCHEMA) shouldBe "tinyint"
  }

  it should "support float" in {
    HiveSchemas.toHiveType(Schema.FLOAT32_SCHEMA) shouldBe "float"
    HiveSchemas.toHiveType(Schema.OPTIONAL_FLOAT32_SCHEMA) shouldBe "float"
  }

  it should "support double" in {
    HiveSchemas.toHiveType(Schema.FLOAT64_SCHEMA) shouldBe "double"
    HiveSchemas.toHiveType(Schema.OPTIONAL_FLOAT64_SCHEMA) shouldBe "double"
  }

  it should "support boolean" in {
    HiveSchemas.toHiveType(Schema.BOOLEAN_SCHEMA) shouldBe "boolean"
  }

  it should "support arrays" in {
    HiveSchemas.toHiveType(SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).build()) shouldBe "array<boolean>"
  }

  it should "support structs" in {
    HiveSchemas.toHiveType(SchemaBuilder.struct()
      .optional()
      .name("mystruct")
      .field("a", Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .field("b", Schema.OPTIONAL_FLOAT32_SCHEMA)
      .build()) shouldBe
      """struct<a:boolean,b:float>"""
  }

  it should "support arrays in structs" in {
    HiveSchemas.toFieldSchemas(
      SchemaBuilder.struct()
        .name("mystruct")
        .field("a", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("b", SchemaBuilder.array(Schema.OPTIONAL_FLOAT32_SCHEMA).build())
        .build())
  }

}
