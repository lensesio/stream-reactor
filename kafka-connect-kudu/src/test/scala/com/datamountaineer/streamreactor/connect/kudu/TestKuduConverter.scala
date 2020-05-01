/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.kudu

import java.util.Collections

import com.datamountaineer.kcql.{Bucketing, Kcql}
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.kudu.client.{KuduTable, Upsert}
import org.mockito.MockitoSugar

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 04/03/16.
  * stream-reactor
  */
//noinspection ScalaDeprecation
class TestKuduConverter extends TestBase with KuduConverter with ConverterUtil with MockitoSugar {
  "Should convert a SinkRecord Schema to Kudu Schema" in {
    val kcql = mock[Kcql]
    val bucketing = mock[Bucketing]
    when(bucketing.getBucketNames).thenReturn(Collections.emptyIterator[String]())
    when(kcql.getBucketing).thenReturn(bucketing)
    val record = getTestRecords.head
    val connectSchema = record.valueSchema()
    val connectFields = connectSchema.fields()
    val kuduSchema = convertToKuduSchema(record, kcql)

    val columns = kuduSchema.getColumns
    columns.size() shouldBe connectFields.size()
  }

  "Should convert a SinkRecord into a Kudu Insert operation" in {
    val kcql = mock[Kcql]
    val bucketing = mock[Bucketing]
    when(bucketing.getBucketNames).thenReturn(Collections.emptyIterator[String]())
    when(kcql.getBucketing).thenReturn(bucketing)
    val record = getTestRecords.head
    val fields = record.valueSchema().fields().asScala.map(f => (f.name(), f.name())).toMap
    val kuduSchema = convertToKuduSchema(record, kcql)
    val kuduRow = kuduSchema.newPartialRow()
    val insert = mock[Upsert]
    when(insert.getRow).thenReturn(kuduRow)
    val table = mock[KuduTable]
    when(table.newUpsert()).thenReturn(insert)
    when(table.getSchema).thenReturn(kuduSchema)
    val converted = convert(record, fields)
    val kuduInsert = convertToKuduUpsert(converted, table)
    kuduRow shouldBe kuduInsert.getRow
  }

  "Should convert a SinkRecord into a Kudu Insert operation with Field Selection" in {
    val kcql = mock[Kcql]
    val bucketing = mock[Bucketing]
    when(bucketing.getBucketNames).thenReturn(Collections.emptyIterator[String]())
    when(kcql.getBucketing).thenReturn(bucketing)
    val fields = Map("id" -> "id", "long_field" -> "new_field_name")
    val record = getTestRecords.head
    val converted = convert(record, fields)
    val kuduSchema = convertToKuduSchema(converted, kcql)
    val kuduRow = kuduSchema.newPartialRow()
    val insert = mock[Upsert]
    when(insert.getRow).thenReturn(kuduRow)
    val table = mock[KuduTable]
    when(table.newUpsert()).thenReturn(insert)
    when(table.getSchema).thenReturn(kuduSchema)
    val kuduInsert = convertToKuduUpsert(converted, table)
    kuduRow shouldBe kuduInsert.getRow
  }


  "Should convert an Avro to Kudu" in {
    val stringSchema = Schema.createUnion(List(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)).asJava)
    val intSchema = Schema.createUnion(List(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)).asJava)
    val booleanSchema = Schema.createUnion(List(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BOOLEAN)).asJava)
    val doubleSchema = Schema.createUnion(List(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.DOUBLE)).asJava)
    val floatSchema = Schema.createUnion(List(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.FLOAT)).asJava)
    val longSchema = Schema.createUnion(List(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG)).asJava)
    val bytesSchema = Schema.createUnion(List(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BYTES)).asJava)

    val schema = SchemaBuilder
      .record("datamountaineer").namespace("com.datamountaineer.connect.kudu")
      .fields()
      .name("string_field").`type`(stringSchema).withDefault(null)
      .name("int_field").`type`(intSchema).withDefault(null)
      .name("boolean_field").`type`(booleanSchema).withDefault(null)
      .name("double_field").`type`(doubleSchema).withDefault(null)
      .name("float_field").`type`(floatSchema).withDefault(null)
      .name("long_field").`type`(longSchema).withDefault(null)
      .name("bytes_field").`type`(bytesSchema).withDefault(null)
      .endRecord()


    val kuduFields = schema.getFields.asScala.map(f => fromAvro(f.schema(), f.name()).build())
    kuduFields.head.getName shouldBe "string_field"
    kuduFields.head.getType shouldBe org.apache.kudu.Type.STRING
    kuduFields(1).getName shouldBe "int_field"
    kuduFields(1).getType shouldBe org.apache.kudu.Type.INT32
    kuduFields(2).getName shouldBe "boolean_field"
    kuduFields(2).getType shouldBe org.apache.kudu.Type.BOOL
    kuduFields(3).getName shouldBe "double_field"
    kuduFields(3).getType shouldBe org.apache.kudu.Type.DOUBLE
    kuduFields(4).getName shouldBe "float_field"
    kuduFields(4).getType shouldBe org.apache.kudu.Type.FLOAT
    kuduFields(5).getName shouldBe "long_field"
    kuduFields(5).getType shouldBe org.apache.kudu.Type.INT64
    kuduFields(6).getName shouldBe "bytes_field"
    kuduFields(6).getType shouldBe org.apache.kudu.Type.BINARY
  }
}
