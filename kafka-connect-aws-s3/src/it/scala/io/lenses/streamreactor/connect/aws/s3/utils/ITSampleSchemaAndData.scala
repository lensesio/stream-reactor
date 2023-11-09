package io.lenses.streamreactor.connect.aws.s3.utils

import io.lenses.streamreactor.connect.aws.s3.model.Topic
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

object ITSampleSchemaAndData extends Matchers {

  // TODO: Reuse these throughout all tests!
  val schema: Schema = SchemaBuilder.struct()
    .field("name", SchemaBuilder.string().required().build())
    .field("title", SchemaBuilder.string().optional().build())
    .field("salary", SchemaBuilder.float64().optional().build())
    .build()

  val UsersSchemaDecimal: Schema = SchemaBuilder.struct()
    .field("name", SchemaBuilder.string().required().build())
    .field("title", SchemaBuilder.string().optional().build())
    .field("salary", Decimal.builder(18).optional().build())
    .build()

  val users: List[Struct] = List(
    new Struct(schema).put("name", "sam").put("title", "mr").put("salary", 100.43),
    new Struct(schema).put("name", "laura").put("title", "ms").put("salary", 429.06),
    new Struct(schema).put("name", "tom").put("title", null).put("salary", 395.44),
    new Struct(schema).put("name", "martin").put("title", "mr").put("salary", 395.44),
    new Struct(schema).put("name", "jackie").put("title", "mrs").put("salary", 395.44),
    new Struct(schema).put("name", "adam").put("title", "mr").put("salary", 395.44),
    new Struct(schema).put("name", "jonny").put("title", "mr").put("salary", 395.44),
    new Struct(schema).put("name", "jim").put("title", "mr").put("salary", 395.44),
    new Struct(schema).put("name", "wilson").put("title", "dog").put("salary", 395.44),
    new Struct(schema).put("name", "milson").put("title", "dog").put("salary", 395.44),
  )

  val firstUsers: List[Struct] = users.slice(0, 3)

  val topic: Topic = Topic("niceTopic")

  def checkRecord(genericRecord: GenericRecord, name: String, title: String, salary: Double): Assertion =
    checkRecord(genericRecord, name, Some(title), salary)

  def checkRecord(genericRecord: GenericRecord, name: String, title: Option[String], salary: Double): Assertion = {
    genericRecord.get("name").toString should be(name)
    Option(genericRecord.get("title")).fold(Option.empty[String])(e => Some(e.toString)) should be(title)
    genericRecord.get("salary") should be(salary)
  }

  def checkArray(genericRecord: GenericData.Array[Utf8], values: String*): Unit =
    values.zipWithIndex.foreach {
      case (string, index) => genericRecord.get(index).toString should be(string)
    }

}
