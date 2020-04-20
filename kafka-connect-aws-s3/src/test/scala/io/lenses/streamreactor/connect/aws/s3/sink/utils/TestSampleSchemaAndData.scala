package io.lenses.streamreactor.connect.aws.s3.sink.utils

import java.io.InputStream

import com.google.common.io.ByteStreams
import io.lenses.streamreactor.connect.aws.s3.Topic
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers


object TestSampleSchemaAndData extends Matchers {

  lazy val firstRecordAsAvro: Array[Byte] = resourceTobyteArray(getClass.getClassLoader.getResourceAsStream("avro/firstRecord.avro"))

  lazy val recordsAsAvro: Array[Byte] = resourceTobyteArray(getClass.getClassLoader.getResourceAsStream("avro/allRecords.avro"))

  // TODO: Reuse these throughout all tests!
  val schema: Schema = SchemaBuilder.struct()
    .field("name", SchemaBuilder.string().required().build())
    .field("title", SchemaBuilder.string().optional().build())
    .field("salary", SchemaBuilder.float64().optional().build())
    .build()

  val users: List[Struct] = List(
    new Struct(schema).put("name", "sam").put("title", "mr").put("salary", 100.43),
    new Struct(schema).put("name", "laura").put("title", "ms").put("salary", 429.06),
    new Struct(schema).put("name", "tom").put("title", null).put("salary", 395.44)
  )

  val topic: Topic = Topic("nicetopic")

  val recordsAsJson: List[String] = List(
    """{"name":"sam","title":"mr","salary":100.43}""",
    """{"name":"laura","title":"ms","salary":429.06}""",
    """{"name":"tom","title":null,"salary":395.44}""",
    ""
  )

  def resourceTobyteArray(inputStream: InputStream): Array[Byte] = {
    ByteStreams.toByteArray(inputStream)
  }

  def checkRecord(genericRecord: GenericRecord, name: String, title: String, salary: Double): Assertion = {

    genericRecord.get("name").toString should be(name)
    genericRecord.get("title").toString should be(title)
    genericRecord.get("salary") should be(salary)
  }

}
