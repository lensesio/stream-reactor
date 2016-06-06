package com.datamountaineer.streamreactor.connect.hbase

import com.datamountaineer.streamreactor.connect.hbase.avro.AvroRecordFieldExtractorMapFn
import com.datamountaineer.streamreactor.connect.rowkeys.{ AvroRecordRowKeyBuilderBytes}
import com.datamountaineer.streamreactor.connect.schemas.BytesHelper._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

class AvroRecordRowKeyBuilderTest extends WordSpec with Matchers with MockitoSugar {
  val schema = new Schema.Parser().parse(PersonAvroSchema.schema)

  "AvroRecordRowKeyBuilder" should {
    "extract the values from the avro record and create the key" in {
      val keys = Seq("firstName", "lastName", "age")
      val rowKeyBuilder = new AvroRecordRowKeyBuilderBytes(AvroRecordFieldExtractorMapFn(schema, keys), keys)

      val sinkRecord = mock[SinkRecord]
      val firstName = "Jack"
      val lastName = "Smith"
      val age = 29

      val record = new GenericRecord {

        val values: Map[String, AnyRef] = Map("firstName" -> firstName, "lastName" -> lastName, "age" -> Int.box(age))

        override def get(key: String): AnyRef = values.get(key).get

        override def put(key: String, v: scala.Any): Unit = sys.error("not supported")

        override def get(i: Int): AnyRef = sys.error("not supported")


        override def put(i: Int, v: scala.Any): Unit = sys.error("not supported")


        override def getSchema: Schema = sys.error("not supported")
      }

      val expectedValue = Bytes.add(
        Array(
          firstName.fromString(),
          rowKeyBuilder.delimBytes,
          lastName.fromString(),
          rowKeyBuilder.delimBytes,
          age.fromInt()))
      rowKeyBuilder.build(sinkRecord, record) shouldBe expectedValue
    }
  }
}
