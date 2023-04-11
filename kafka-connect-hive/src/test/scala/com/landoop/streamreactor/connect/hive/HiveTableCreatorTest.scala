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
package com.landoop.streamreactor.connect.hive

import com.landoop.streamreactor.connect.hive
import com.landoop.streamreactor.connect.hive.formats.HiveFormat
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.kafka.connect.data.SchemaBuilder
import org.mockito.Mockito.RETURNS_DEEP_STUBS
import org.mockito.MockitoSugar
import org.mockito.captor.ArgCaptor
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HiveTableCreatorTest extends AnyFlatSpec with MockitoSugar with Matchers {

  private val db        = DatabaseName("awesomeDatabase")
  private val tableName = TableName("awsomeTable")
  private val schema = SchemaBuilder.struct()
    .field("department_number", SchemaBuilder.int64().required().build())
    .field("student_name", SchemaBuilder.string().optional().build())
    .build()
  private val partitions = Seq(PartitionField("department_number"))
  private val location   = None
  private val format     = HiveFormat.apply("orc")
  private implicit val client:     IMetaStoreClient = mock[IMetaStoreClient](RETURNS_DEEP_STUBS)
  private implicit val fileSystem: FileSystem       = mock[FileSystem]

  when(client.getDatabase(db.value).getLocationUri).thenReturn("http://some-hive-server.com/location")
  val tableArgCaptor = ArgCaptor[Table]

  "createTable" should "work with partitions" in {

    hive.createTable(db, tableName, schema, partitions, location, format)

    verify(client).createTable(tableArgCaptor.capture)

    val fieldSchema = tableArgCaptor.value.getPartitionKeys.get(0)
    fieldSchema.getName should be("department_number")
    fieldSchema.getType should be("bigint")

    val cols = tableArgCaptor.value.getSd.getCols
    cols.size() should be(1)
    val colFieldSchema = cols.get(0)
    colFieldSchema.getName should be("student_name")
    colFieldSchema.getType should be("string")
  }

  "createTable" should "throw error when no schema field available" in {

    val schemaNoField = SchemaBuilder.struct()
      .field("department_no", SchemaBuilder.int64().required().build())
      .field("student_name", SchemaBuilder.string().optional().build())
      .build()

    val caught = intercept[IllegalArgumentException] {
      hive.createTable(db, tableName, schemaNoField, partitions, location, format)
    }
    caught.getMessage should be("No field available in schema for defined partition 'department_number'")

  }
}
