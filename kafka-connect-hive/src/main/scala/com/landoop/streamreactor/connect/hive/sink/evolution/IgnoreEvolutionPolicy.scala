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
package com.landoop.streamreactor.connect.hive.sink.evolution

import com.landoop.streamreactor.connect.hive.DatabaseName
import com.landoop.streamreactor.connect.hive.HiveSchemas
import com.landoop.streamreactor.connect.hive.TableName
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.errors.ConnectException

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.Try

/**
  * An compile of [[EvolutionPolicy]] that requires the
  * input schema be equal or a superset of the metastore schema.
  *
  * This means that every field in the metastore schema must be
  * present in the incoming records, but the records may have
  * additional fields. These additional fields will be dropped
  * before the data is written out.
  */
object IgnoreEvolutionPolicy extends EvolutionPolicy {

  override def evolve(
    dbName:          DatabaseName,
    tableName:       TableName,
    metastoreSchema: Schema,
    inputSchema:     Schema,
  )(
    implicit
    client: IMetaStoreClient,
  ): Try[Schema] = Try {
    HiveSchemas.toKafka(client.getTable(dbName.value, tableName.value))
  }.map { schema =>
    val compatible = schema.fields().asScala.forall { field =>
      inputSchema.field(field.name) != null ||
      field.schema().isOptional ||
      field.schema().defaultValue() != null
    }
    if (compatible) schema else throw new ConnectException("Input Schema is not compatible with the metastore")
  }
}
