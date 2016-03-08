package com.datamountaineer.connect.utils

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

/**
  * Created by andrew@datamountaineer.com on 29/02/16. 
  * stream-reactor
  */

trait TestUtilsBase extends FunSuite with Matchers with BeforeAndAfter {
  val TOPIC = "sink_test"
  val VALUE_JSON_STRING="{\"id\":\"sink_test-1-1\",\"int_field\":1,\"long_field\":1,\"string_field\":\"foo\"}"
  val KEY="topic_key_1"
  val ID = "sink_test-1-1"
  val AVRO_SCHEMA_LITERAL = "{\n\t\"type\": \"record\",\n\t\"name\": \"myrecord\",\n\t\"fields\": [{\n\t\t\"name\": \"id\",\n\t\t\"type\": \"string\"\n\t}, {\n\t\t\"name\": \"int_field\",\n\t\t\"type\": \"int\"\n\t}, {\n\t\t\"name\": \"long_field\",\n\t\t\"type\": \"long\"\n\t}, {\n\t\t\"name\": \"string_field\",\n\t\t\"type\": \"string\"\n\t}]\n}"
  val AVRO_SCHEMA : org.apache.avro.Schema = new org.apache.avro.Schema.Parser().parse(AVRO_SCHEMA_LITERAL)

  def buildAvro() : GenericRecord = {
    val avro = new GenericData.Record(AVRO_SCHEMA)
    avro.put("id", ID)
    avro.put("int_field", 1)
    avro.put("long_field", 1L)
    avro.put("string_field", "foo")
    avro
  }

  //build a test record schema
  def createSchema: Schema = {
    SchemaBuilder.struct.name("record")
      .version(1)
      .field("id", Schema.STRING_SCHEMA)
      .field("int_field", Schema.INT32_SCHEMA)
      .field("long_field", Schema.INT64_SCHEMA)
      .field("string_field", Schema.STRING_SCHEMA)
      .build
  }

  //build a test record
  def createRecord(schema: Schema, id: String): Struct = {
    new Struct(schema)
      .put("id", id)
      .put("int_field", 1)
      .put("long_field", 1L)
      .put("string_field", "foo")
  }

  //generate some test records
  def getTestRecord: SinkRecord= {
    val schema = createSchema
    val record: Struct = createRecord(schema, ID)
    new SinkRecord(TOPIC, 1, Schema.STRING_SCHEMA, KEY.toString, schema, record, 1)
  }
}