package com.datamountaineer.streamreactor.connect.hbase.avro

import com.datamountaineer.streamreactor.connect.hbase.PersonAvroSchema
import org.apache.avro.Schema
import org.scalatest.{Matchers, WordSpec}


class AvroSchemaFieldsExistFnTest extends WordSpec with Matchers {
  val schema = new Schema.Parser().parse(PersonAvroSchema.schema)

  "AvroSchemaFieldsExistFn" should {
    "raise an exception if the field is not present" in {
      intercept[IllegalArgumentException] {
        AvroSchemaFieldsExistFn(schema, Seq("notpresent"))
      }

      intercept[IllegalArgumentException] {
        AvroSchemaFieldsExistFn(schema, Seq(" lastName"))
      }
    }

    "not raise an exception if the fields are present" in {
      AvroSchemaFieldsExistFn(schema, Seq("lastName", "age", "address"))
    }
  }
}
