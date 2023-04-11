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
import com.landoop.streamreactor.connect.hive.TableName
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.kafka.connect.data.Schema

import scala.util.Try

/**
  * An compile of [[EvolutionPolicy]] that peforms no checks.
  *
  * This means that invalid data may be written and/or exceptions may be thrown.
  *
  * This policy can be useful in tests but should be avoided in production code.
  */
object NoopEvolutionPolicy extends EvolutionPolicy {
  override def evolve(
    dbName:          DatabaseName,
    tableName:       TableName,
    metastoreSchema: Schema,
    inputSchema:     Schema,
  )(
    implicit
    client: IMetaStoreClient,
  ): Try[Schema] = Try(metastoreSchema)
}
