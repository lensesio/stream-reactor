package com.landoop.streamreactor.connect.hive.sink.mapper

import org.apache.kafka.connect.data.{SchemaBuilder, Struct}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class MetastoreSchemaAlignMapperTest extends AnyFunSuite with Matchers {

  test("pad optional missing fields with null") {

    val recordSchema = SchemaBuilder.struct()
      .field("a", SchemaBuilder.string().required().build())
      .field("b", SchemaBuilder.string().required().build())
      .field("c", SchemaBuilder.string().required().build())
      .build()

    val struct = new Struct(recordSchema).put("a", "a").put("b", "b").put("c", "c")

    val metastoreSchema = SchemaBuilder.struct()
      .field("a", SchemaBuilder.string().required().build())
      .field("b", SchemaBuilder.string().required().build())
      .field("c", SchemaBuilder.string().required().build())
      .field("z", SchemaBuilder.string().optional().build())
      .build()

    val output = new MetastoreSchemaAlignMapper(metastoreSchema).map(struct)
    output.schema().fields().asScala.map(_.name) shouldBe Seq("a", "b", "c", "z")
  }

  test("drop fields not specified in metastore") {

    val recordSchema = SchemaBuilder.struct()
      .field("a", SchemaBuilder.string().required().build())
      .field("b", SchemaBuilder.string().required().build())
      .field("c", SchemaBuilder.string().required().build())
      .build()

    val struct = new Struct(recordSchema).put("a", "a").put("b", "b").put("c", "c")

    val metastoreSchema = SchemaBuilder.struct()
      .field("a", SchemaBuilder.string().required().build())
      .field("b", SchemaBuilder.string().required().build())
      .build()

    val output = new MetastoreSchemaAlignMapper(metastoreSchema).map(struct)
    output.schema().fields().asScala.map(_.name) shouldBe Seq("a", "b")
  }
}
