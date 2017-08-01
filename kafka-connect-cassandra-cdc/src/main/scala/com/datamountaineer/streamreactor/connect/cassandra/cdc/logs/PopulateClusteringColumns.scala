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
package com.datamountaineer.streamreactor.connect.cassandra.cdc.logs

import com.datamountaineer.streamreactor.connect.cassandra.cdc.config.CdcConfig
import com.datamountaineer.streamreactor.connect.cassandra.cdc.metadata.ConnectSchemaBuilder
import org.apache.cassandra.config.CFMetaData
import org.apache.cassandra.db.Clustering
import org.apache.kafka.connect.data.Struct

/**
  * Sets the values for clustering columns on the given change struct.
  */
object PopulateClusteringColumns {
  def apply(struct: Struct,
            metadata: CFMetaData,
            clustering: Clustering)(implicit config: CdcConfig): Unit = {
    val clusteringColumns = metadata.clusteringColumns()

    (0 until clustering.size())
      .foreach { i =>
        val col = clusteringColumns.get(i)

        val value = col.`type`.getSerializer.deserialize(clustering.get(i))
        val coerced = ConnectSchemaBuilder.coerceValue(
          value,
          col.`type`,
          struct.schema().field(col.name.toString).schema()
        )
        struct.put(col.name.toString, coerced)
      }
  }
}
