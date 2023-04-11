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

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.Try

/**
  * An compile of [[EvolutionPolicy]] that attempts to evolve
  * the metastore schema to match the input schema by adding missing fields.
  */
object AddEvolutionPolicy extends EvolutionPolicy {

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  override def evolve(
    dbName:          DatabaseName,
    tableName:       TableName,
    metastoreSchema: Schema,
    inputSchema:     Schema,
  )(
    implicit
    client: IMetaStoreClient,
  ): Try[Schema] = Try {

    val missing = inputSchema.fields.asScala
      .filter(f => metastoreSchema.field(f.name) == null)
      .map(HiveSchemas.toFieldSchema)

    if (missing.nonEmpty) {
      logger.info(s"Evolving hive metastore to add: ${missing.mkString(",")}")

      val table = client.getTable(dbName.value, tableName.value)
      val cols  = table.getSd.getCols
      missing.foreach(field => cols.add(field))
      table.getSd.setCols(cols)
      client.alter_table(dbName.value, tableName.value, table)

      HiveSchemas.toKafka(client.getTable(dbName.value, tableName.value))

    } else {
      metastoreSchema
    }
  }
}
