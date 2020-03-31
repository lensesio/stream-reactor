package com.landoop.streamreactor.connect.hive.sink.mapper

import cats.data.NonEmptyList
import com.landoop.streamreactor.connect.hive.{PartitionKey, PartitionPlan, TableName}
import org.apache.kafka.connect.data.{SchemaBuilder, Struct}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class DropPartitionValuesMapperTest extends AnyFunSuite with Matchers {

  test("strip partition values") {

    val schema = SchemaBuilder.struct()
      .field("a", SchemaBuilder.string().required().build())
      .field("p", SchemaBuilder.string().required().build())
      .field("q", SchemaBuilder.string().required().build())
      .field("z", SchemaBuilder.string().required().build())
      .build()

    val plan = PartitionPlan(TableName("foo"), NonEmptyList.of(PartitionKey("p"), PartitionKey("q")))
    val struct = new Struct(schema).put("a", "a").put("p", "p").put("q", "q").put("z", "z")
    val output = new DropPartitionValuesMapper(plan).map(struct)
    output.schema().fields().asScala.map(_.name) shouldBe Seq("a", "z")
  }

  test("handle partition field is missing in input") {

    val schema = SchemaBuilder.struct()
      .field("a", SchemaBuilder.string().required().build())
      .field("q", SchemaBuilder.string().required().build())
      .field("z", SchemaBuilder.string().required().build())
      .build()


    val plan = PartitionPlan(TableName("foo"), NonEmptyList.of(PartitionKey("p"), PartitionKey("q")))
    val struct = new Struct(schema).put("a", "a").put("q", "q").put("z", "z")
    val output = new DropPartitionValuesMapper(plan).map(struct)
    output.schema().fields().asScala.map(_.name) shouldBe Seq("a", "z")
  }
}
