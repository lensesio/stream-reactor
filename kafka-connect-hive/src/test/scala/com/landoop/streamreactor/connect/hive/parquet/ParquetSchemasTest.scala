package com.landoop.streamreactor.connect.hive.parquet

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.Types
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ParquetSchemasTest extends AnyFlatSpec with Matchers {

  "ParquetSchemas.toKafka" should "support boolean" in {
    ParquetSchemas.toKafka(
      Types.optional(PrimitiveTypeName.BOOLEAN).named("foo"),
    ) shouldBe Schema.OPTIONAL_BOOLEAN_SCHEMA
  }

  it should "support tinyint" in {
    ParquetSchemas.toKafka(
      Types.optional(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.intType(8, true)).named("foo"),
    ) shouldBe Schema.OPTIONAL_INT8_SCHEMA
    ParquetSchemas.toKafka(
      Types.optional(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.intType(8, false)).named("foo"),
    ) shouldBe Schema.OPTIONAL_INT8_SCHEMA
  }

  it should "support smallint" in {
    ParquetSchemas.toKafka(
      Types.optional(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.intType(16, true)).named("foo"),
    ) shouldBe Schema.OPTIONAL_INT16_SCHEMA
    ParquetSchemas.toKafka(
      Types.optional(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.intType(16, false)).named("foo"),
    ) shouldBe Schema.OPTIONAL_INT16_SCHEMA
  }

  it should "support 32 bit ints" in {
    ParquetSchemas.toKafka(Types.optional(PrimitiveTypeName.INT32).named("foo")) shouldBe Schema.OPTIONAL_INT32_SCHEMA
    ParquetSchemas.toKafka(
      Types.optional(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.intType(32, true)).named("foo"),
    ) shouldBe Schema.OPTIONAL_INT32_SCHEMA
    ParquetSchemas.toKafka(
      Types.optional(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.intType(32, false)).named("foo"),
    ) shouldBe Schema.OPTIONAL_INT32_SCHEMA
    ParquetSchemas.toKafka(Types.required(PrimitiveTypeName.INT32).named("foo")) shouldBe Schema.INT32_SCHEMA
    ParquetSchemas.toKafka(
      Types.required(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.intType(32, true)).named("foo"),
    ) shouldBe Schema.INT32_SCHEMA
    ParquetSchemas.toKafka(
      Types.required(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.intType(32, false)).named("foo"),
    ) shouldBe Schema.INT32_SCHEMA
  }

  it should "support 64 bit longs" in {
    ParquetSchemas.toKafka(Types.optional(PrimitiveTypeName.INT64).named("foo")) shouldBe Schema.OPTIONAL_INT64_SCHEMA
    ParquetSchemas.toKafka(
      Types.optional(PrimitiveTypeName.INT64).as(LogicalTypeAnnotation.intType(64, true)).named("foo"),
    ) shouldBe Schema.OPTIONAL_INT64_SCHEMA
    ParquetSchemas.toKafka(
      Types.optional(PrimitiveTypeName.INT64).as(LogicalTypeAnnotation.intType(64, false)).named("foo"),
    ) shouldBe Schema.OPTIONAL_INT64_SCHEMA
    ParquetSchemas.toKafka(Types.required(PrimitiveTypeName.INT64).named("foo")) shouldBe Schema.INT64_SCHEMA
    ParquetSchemas.toKafka(
      Types.required(PrimitiveTypeName.INT64).as(LogicalTypeAnnotation.intType(64, true)).named("foo"),
    ) shouldBe Schema.INT64_SCHEMA
    ParquetSchemas.toKafka(
      Types.required(PrimitiveTypeName.INT64).as(LogicalTypeAnnotation.intType(64, false)).named("foo"),
    ) shouldBe Schema.INT64_SCHEMA
  }

  it should "support double" in {
    ParquetSchemas.toKafka(
      Types.optional(PrimitiveTypeName.DOUBLE).named("foo"),
    ) shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    ParquetSchemas.toKafka(Types.required(PrimitiveTypeName.DOUBLE).named("foo")) shouldBe Schema.FLOAT64_SCHEMA
  }

  it should "support float" in {
    ParquetSchemas.toKafka(Types.optional(PrimitiveTypeName.FLOAT).named("foo")) shouldBe Schema.OPTIONAL_FLOAT32_SCHEMA
    ParquetSchemas.toKafka(Types.required(PrimitiveTypeName.FLOAT).named("foo")) shouldBe Schema.FLOAT32_SCHEMA
  }

  it should "support strings" in {
    ParquetSchemas.toKafka(
      Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("foo"),
    ) shouldBe Schema.OPTIONAL_STRING_SCHEMA
    ParquetSchemas.toKafka(
      Types.optional(PrimitiveTypeName.BINARY).length(32).as(LogicalTypeAnnotation.stringType()).named("foo"),
    ) shouldBe Schema.OPTIONAL_STRING_SCHEMA
    ParquetSchemas.toKafka(
      Types.required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("foo"),
    ) shouldBe Schema.STRING_SCHEMA
    ParquetSchemas.toKafka(
      Types.required(PrimitiveTypeName.BINARY).length(32).as(LogicalTypeAnnotation.stringType()).named("foo"),
    ) shouldBe Schema.STRING_SCHEMA
  }

  // DATE is used to for a logical date type, without a time of day.
  // It must annotate an int32 that stores the number of days from the Unix epoch, 1 January 1970.
  it should "support date" in {
    ParquetSchemas.toKafka(
      Types.optional(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.dateType()).named("foo"),
    ) shouldBe Schema.OPTIONAL_INT32_SCHEMA
  }

  it should "support arrays" in {
    val group = Types.requiredList().element(Types.required(PrimitiveTypeName.FLOAT).named("element")).named("foo")
    ParquetSchemas.toKafka(group) shouldBe SchemaBuilder.array(Schema.FLOAT32_SCHEMA).build()
  }

  it should "support structs" in {
    ParquetSchemas.toKafka(
      Types.buildMessage().addFields(
        Types.optional(PrimitiveTypeName.FLOAT).named("a"),
        Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("b"),
      ).named("foo"),
    ) shouldBe SchemaBuilder.struct().name("foo")
      .field("a", Schema.OPTIONAL_FLOAT32_SCHEMA)
      .field("b", Schema.OPTIONAL_STRING_SCHEMA).build()
  }

  it should "support nested structs" in {}

  it should "support structs with arrays" in {
    val parquet = Types.buildMessage().addFields(
      Types.required(PrimitiveTypeName.FLOAT).named("a"),
      Types.optionalList().element(
        Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("element"),
      ).named("b"),
    ).named("foo")
    val expected = SchemaBuilder.struct().name("foo")
      .field("a", Schema.FLOAT32_SCHEMA)
      .field("b", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build()).build()
    val actual = ParquetSchemas.toKafka(parquet)
    actual shouldBe expected
  }

  "ParquetSchemas.toParquetType" should "support strings" in {
    ParquetSchemas.toParquetType(Schema.STRING_SCHEMA, "foo") shouldBe Types.required(PrimitiveTypeName.BINARY).as(
      LogicalTypeAnnotation.stringType(),
    ).named("foo")
    ParquetSchemas.toParquetType(Schema.OPTIONAL_STRING_SCHEMA, "foo") shouldBe Types.optional(
      PrimitiveTypeName.BINARY,
    ).as(LogicalTypeAnnotation.stringType()).named("foo")
  }

  it should "support arrays" in {
    ParquetSchemas.toParquetType(SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).optional().build(), "foo") shouldBe
      Types.optionalList().element(PrimitiveTypeName.BOOLEAN, Repetition.REQUIRED).named("foo")
  }

  it should "support top level structs" in {

    val schema = SchemaBuilder.struct()
      .field("a", Schema.OPTIONAL_STRING_SCHEMA)
      .field("b", Schema.BOOLEAN_SCHEMA)
      .build()

    ParquetSchemas.toParquetMessage(schema, "foo") shouldBe
      Types.buildMessage().addFields(
        Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("a"),
        Types.required(PrimitiveTypeName.BOOLEAN).named("b"),
      ).named("foo")
  }

  it should "support optional nested structs" in {

    val schema = SchemaBuilder.struct()
      .field("a", Schema.OPTIONAL_FLOAT32_SCHEMA)
      .field(
        "b",
        SchemaBuilder.struct()
          .field("c", Schema.BOOLEAN_SCHEMA)
          .field("d", Schema.OPTIONAL_FLOAT64_SCHEMA)
          .optional()
          .build(),
      )
      .build()

    ParquetSchemas.toParquetMessage(schema, "wobble") shouldBe
      Types.buildMessage().addFields(
        Types.optional(PrimitiveTypeName.FLOAT).named("a"),
        Types.optionalGroup().addFields(
          Types.required(PrimitiveTypeName.BOOLEAN).named("c"),
          Types.optional(PrimitiveTypeName.DOUBLE).named("d"),
        ).named("b"),
      ).named("wobble")
  }
}
