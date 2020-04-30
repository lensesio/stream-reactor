package com.datamountaineer.kcql

import cats.data.NonEmptyList
import com.landoop.streamreactor.connect.hive.sink.mapper.ProjectionMapper
import org.apache.kafka.connect.data.{SchemaBuilder, Struct}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class ProjectionMapperTest extends AnyFunSuite with Matchers {

  test("drop fields not specified in the projection") {

    val schema = SchemaBuilder.struct()
      .field("a", SchemaBuilder.string().required().build())
      .field("b", SchemaBuilder.string().required().build())
      .field("c", SchemaBuilder.string().required().build())
      .build()

    val struct = new Struct(schema).put("a", "a").put("b", "b").put("c", "c")

    val projection = NonEmptyList.of(new Field("a", "a", FieldType.VALUE), new Field("b", "b", FieldType.VALUE))

    val output = new ProjectionMapper(projection).map(struct)
    output.schema().fields().asScala.map(_.name) shouldBe Seq("a", "b")
  }

  test("rename fields to use an alias") {

    val schema = SchemaBuilder.struct()
      .field("a", SchemaBuilder.string().required().build())
      .field("b", SchemaBuilder.string().required().build())
      .build()

    val struct = new Struct(schema).put("a", "a").put("b", "b")

    val projection = NonEmptyList.of(new Field("a", "a", FieldType.VALUE), new Field("b", "z", FieldType.VALUE))

    val output = new ProjectionMapper(projection).map(struct)
    output.schema().fields().asScala.map(_.name) shouldBe Seq("a", "z")
  }

  test("throw exception if a projection field is missing") {

    val schema = SchemaBuilder.struct()
      .field("a", SchemaBuilder.string().required().build())
      .build()

    val struct = new Struct(schema).put("a", "a")

    val projection = NonEmptyList.of(new Field("a", "a", FieldType.VALUE), new Field("b", "b", FieldType.VALUE))

    intercept[RuntimeException] {
      new ProjectionMapper(projection).map(struct)
    }.getMessage shouldBe "Missing field b"
  }
}
