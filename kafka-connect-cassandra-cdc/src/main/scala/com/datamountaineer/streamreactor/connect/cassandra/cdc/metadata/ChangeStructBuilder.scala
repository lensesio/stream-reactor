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
package com.datamountaineer.streamreactor.connect.cassandra.cdc.metadata

import com.datamountaineer.streamreactor.connect.cassandra.cdc.config.CdcConfig
import org.apache.cassandra.config.CFMetaData
import org.apache.kafka.connect.data.{Schema, SchemaBuilder}

import scala.collection.JavaConversions._

/**
  * Encapsulates a Cassandra mutation information.
  */
object ChangeStructBuilder {
  val KeyspaceField = "keyspace"
  val TableField = "table"
  val ChangeTypeField = "changeType"
  val KeysField = "keys"
  val TimestampField = "timestamp"
  val DeletedColumnsField = "deleted_columns"

  def apply(cf: CFMetaData)(implicit config: CdcConfig): Schema = {

    val builder = SchemaBuilder.struct().name(cf.cfName)
      .field(KeyspaceField, Schema.STRING_SCHEMA)
      .field(TableField, Schema.STRING_SCHEMA)
      .field(ChangeTypeField, Schema.STRING_SCHEMA)
      .field(DeletedColumnsField, SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())

    val pkBuilder = SchemaBuilder.struct().name("primarykeys")

    cf.primaryKeyColumns()
      .filter { col => col.isPartitionKey }
      .foreach { col =>
        pkBuilder.field(col.name.toString, ConnectSchemaBuilder.fromType(col.cellValueType()))
      }

    cf.clusteringColumns().foreach { col =>
      pkBuilder.field(col.name.toString, ConnectSchemaBuilder.fromType(col.cellValueType()))
    }

    builder.field(KeysField, pkBuilder.build())
    builder.field(TimestampField, Schema.INT64_SCHEMA)

    builder.build()
  }
}
