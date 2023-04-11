/*
 * Copyright 2017-2023 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datamountaineer.streamreactor.common

import com.sksamuel.avro4s.RecordFormat
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.header.ConnectHeaders
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.source.SourceTaskContext
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util
import java.util.Collections
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

/**
  * Created by andrew@datamountaineer.com on 29/02/16.
  * stream-reactor
  */

trait TestUtilsBase extends AnyWordSpec with Matchers with BeforeAndAfter with MockitoSugar {
  val TOPIC             = "sink_test"
  val VALUE_JSON_STRING = "{\"id\":\"sink_test-1-1\",\"int_field\":1,\"long_field\":1,\"string_field\":\"foo\"}"
  val KEY               = "topic_key_1"
  val ID                = "sink_test-1-1"
  val AVRO_SCHEMA_LITERAL =
    "{\n\t\"type\": \"record\",\n\t\"name\": \"myrecord\",\n\t\"fields\": [{\n\t\t\"name\": \"id\",\n\t\t\"type\": \"string\"\n\t}, {\n\t\t\"name\": \"int_field\",\n\t\t\"type\": \"int\"\n\t}, {\n\t\t\"name\": \"long_field\",\n\t\t\"type\": \"long\"\n\t}, {\n\t\t\"name\": \"string_field\",\n\t\t\"type\": \"string\"\n\t}]\n}"
  val AVRO_SCHEMA: org.apache.avro.Schema = new org.apache.avro.Schema.Parser().parse(AVRO_SCHEMA_LITERAL)

  def buildAvro(): GenericRecord = {
    val avro = new GenericData.Record(AVRO_SCHEMA)
    avro.put("id", ID)
    avro.put("int_field", 1)
    avro.put("long_field", 1L)
    avro.put("string_field", "foo")
    avro
  }

  def buildNestedAvro(): GenericRecord = {
    val recordFormat = RecordFormat[WithNested]
    val record       = WithNested(1.1, Nested("abc", 100))
    recordFormat.to(record)
  }

  //build a test record schema
  def createSchema: Schema =
    SchemaBuilder.struct.name("record")
      .version(1)
      .field("id", Schema.STRING_SCHEMA)
      .field("int_field", Schema.INT32_SCHEMA)
      .field("long_field", Schema.INT64_SCHEMA)
      .field("string_field", Schema.STRING_SCHEMA)
      .build

  def createKeySchema: Schema =
    SchemaBuilder.struct.name("record")
      .version(1)
      .field("key_id", Schema.STRING_SCHEMA)
      .field("key_int_field", Schema.INT32_SCHEMA)
      .field("key_long_field", Schema.INT64_SCHEMA)
      .field("key_string_field", Schema.STRING_SCHEMA)
      .build

  //build a test record
  def createKeyStruct(schema: Schema, id: String): Struct =
    new Struct(schema)
      .put("key_id", id)
      .put("key_int_field", 1)
      .put("key_long_field", 1L)
      .put("key_string_field", "foo")

  //build a test record
  def createRecord(schema: Schema, id: String): Struct =
    new Struct(schema)
      .put("id", id)
      .put("int_field", 1)
      .put("long_field", 1L)
      .put("string_field", "foo")

  //generate some test records
  def getTestRecord: SinkRecord = {
    val schema = createSchema
    val record: Struct = createRecord(schema, ID)
    new SinkRecord(TOPIC, 1, Schema.STRING_SCHEMA, KEY.toString, schema, record, 1)
  }

  def getSourceTaskContext(lookupPartitionKey: String, offsetValue: String, offsetColumn: String, table: String) = {

    /**
      * offset holds a map of map[string, something],map[identifier, value]
      *
      * map(map(assign.import.table->table1) -> map("my_timeuuid"->"2013-01-01 00:05+0000")
      */

    //set up partition
    val partition: util.Map[String, String] = Collections.singletonMap(lookupPartitionKey, table)
    //as a list to search for
    val partitionList: util.List[util.Map[String, String]] = List(partition).asJava
    //set up the offset
    val offset: util.Map[String, Object] = Collections.singletonMap(offsetColumn, offsetValue)
    //create offsets to initialize from
    val offsets: util.Map[util.Map[String, String], util.Map[String, Object]] = Map(partition -> offset).asJava

    //mock out reader and task context
    val taskContext = mock[SourceTaskContext]
    val reader      = mock[OffsetStorageReader]
    when(reader.offsets(partitionList)).thenReturn(offsets)
    when(taskContext.offsetStorageReader()).thenReturn(reader)

    taskContext
  }

  def sinkRecordWithKeyHeaders(): SinkRecord = {
    val baseRecords = getTestRecord
    val keySchema: Schema = createKeySchema
    val keyStruct: Struct = createKeyStruct(keySchema, ID)
    val _:         Schema = createSchema
    val headers = new ConnectHeaders()
    headers.addString("header_field_1", "foo")
    headers.addString("header_field_2", "bar")
    headers.addString("header_field_3", "boo")

    baseRecords.newRecord(
      baseRecords.topic(),
      baseRecords.kafkaPartition(),
      keySchema,
      keyStruct,
      baseRecords.valueSchema(),
      baseRecords.value(),
      baseRecords.timestamp(),
      headers,
    )
  }
}

case class WithNested(x: Double, y: Nested)
case class Nested(a: String, b: Int)
