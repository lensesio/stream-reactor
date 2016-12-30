/*
 * *
 *   * Copyright 2016 Datamountaineer.
 *   *
 *   * Licensed under the Apache License, Version 2.0 (the "License");
 *   * you may not use this file except in compliance with the License.
 *   * You may obtain a copy of the License at
 *   *
 *   * http://www.apache.org/licenses/LICENSE-2.0
 *   *
 *   * Unless required by applicable law or agreed to in writing, software
 *   * distributed under the License is distributed on an "AS IS" BASIS,
 *   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   * See the License for the specific language governing permissions and
 *   * limitations under the License.
 *   *
 */

package com.datamountaineer.streamreactor.connect.kudu

import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.codehaus.jackson.node.NullNode
import org.kududb.client.{KuduTable, Upsert}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * Created by andrew@datamountaineer.com on 04/03/16. 
  * stream-reactor
  */
class TestKuduConverter extends TestBase with KuduConverter with ConverterUtil with MockitoSugar {
  "Should convert a SinkRecord Schema to Kudu Schema" in {
    val record = getTestRecords.head
    val connectSchema = record.valueSchema()
    val connectFields = connectSchema.fields()
    val kuduSchema = convertToKuduSchema(record)

    val columns = kuduSchema.getColumns
    columns.size() shouldBe connectFields.size()
  }

  "Should convert a SinkRecord into a Kudu Insert operation" in {
    val record = getTestRecords.head
    val fields = record.valueSchema().fields().asScala.map(f=>(f.name(), f.name())).toMap
    val kuduSchema = convertToKuduSchema(record)
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
    val fields = Map("id"->"id", "long_field"->"new_field_name")
    val record = getTestRecords.head
    val converted = convert(record, fields)
    val kuduSchema = convertToKuduSchema(converted)
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
    var new_fields = new ListBuffer[Field]()
    new_fields += new Field("string_field", Schema.createUnion(List(Schema.create(Schema.Type.NULL),
      Schema.create(Schema.Type.STRING)).asJava), null, NullNode.getInstance())

    new_fields += new Field("int_field", Schema.createUnion(List(Schema.create(Schema.Type.NULL),
      Schema.create(Schema.Type.INT)).asJava), null, NullNode.getInstance())

    new_fields += new Field("boolean_field", Schema.createUnion(List(Schema.create(Schema.Type.NULL),
      Schema.create(Schema.Type.BOOLEAN)).asJava), null, NullNode.getInstance())

    new_fields += new Field("double_field", Schema.createUnion(List(Schema.create(Schema.Type.NULL),
      Schema.create(Schema.Type.DOUBLE)).asJava), null, NullNode.getInstance())

    new_fields += new Field("float_field", Schema.createUnion(List(Schema.create(Schema.Type.NULL),
      Schema.create(Schema.Type.FLOAT)).asJava), null, NullNode.getInstance())

    new_fields += new Field("long_field", Schema.createUnion(List(Schema.create(Schema.Type.NULL),
      Schema.create(Schema.Type.LONG)).asJava), null, NullNode.getInstance())

    new_fields += new Field("bytes_field", Schema.createUnion(List(Schema.create(Schema.Type.NULL),
      Schema.create(Schema.Type.BYTES)).asJava), null, NullNode.getInstance())


    val kuduFields = new_fields.map(f=>fromAvro(f.schema(), f.name()).build())
    kuduFields.head.getName shouldBe "string_field"
    kuduFields.head.getType shouldBe org.kududb.Type.STRING
    kuduFields(1).getName shouldBe "int_field"
    kuduFields(1).getType shouldBe org.kududb.Type.INT32
    kuduFields(2).getName shouldBe "boolean_field"
    kuduFields(2).getType shouldBe org.kududb.Type.BOOL
    kuduFields(3).getName shouldBe "double_field"
    kuduFields(3).getType shouldBe org.kududb.Type.FLOAT
    kuduFields(4).getName shouldBe "float_field"
    kuduFields(4).getType shouldBe org.kududb.Type.FLOAT
    kuduFields(5).getName shouldBe "long_field"
    kuduFields(5).getType shouldBe org.kududb.Type.INT64
    kuduFields(6).getName shouldBe "bytes_field"
    kuduFields(6).getType shouldBe org.kududb.Type.BINARY
  }
}
