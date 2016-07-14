package com.datamountaineer.streamreactor.connect.hbase.avro

import java.nio.file.Paths

import org.apache.avro.Schema
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.{Matchers, WordSpec}

class AvroRecordFieldExtractorMapFnTest extends WordSpec with Matchers {

  val schema = new Schema.Parser().parse(Paths.get(getClass.getResource("/person.avsc").toURI).toFile)

  "AvroRecordFieldExtractorMapFn" should {
    "raise an exception if the given field does not exist in the schema" in {
      intercept[IllegalArgumentException] {
        AvroRecordFieldExtractorMapFn(schema, Seq("wrongField"))
      }
    }

    "raise an exception if the given field is not a primitive" in {
      intercept[IllegalArgumentException] {
        AvroRecordFieldExtractorMapFn(schema, Seq("address"))
      }
    }

    "create the mappings for all the given fields" in {
      val mappings = AvroRecordFieldExtractorMapFn(schema, Seq("firstName", "age"))

      val fnFirstName = mappings("firstName")
      val firstName = "Beaky"
      fnFirstName(firstName) shouldBe Bytes.toBytes(firstName)

      val fnAge = mappings("age")
      val age = 31
      fnAge(age) shouldBe Bytes.toBytes(age)
      intercept[ClassCastException] {
        fnAge(12.4)
      }
    }
  }
}
