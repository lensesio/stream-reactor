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
import org.apache.cassandra.db.partitions.PartitionUpdate
import org.apache.kafka.connect.data.Struct

import scala.collection.JavaConversions._

/**
  * Sets the values for the primary key columns on the given Kafka Connect Struct.
  */
object PopulatePKColumns {
  def apply(struct: Struct, cf: CFMetaData, pu: PartitionUpdate)(implicit config:CdcConfig): Unit = {
    cf.partitionKeyColumns()
      .map { cd =>
        val value = cd.cellValueType().getSerializer.deserialize(pu.partitionKey().getKey)
        val coerced = ConnectSchemaBuilder.coerceValue(
          value,
          cd.cellValueType(),
          struct.schema().field(cd.name.toString).schema()
        )

        struct.put(cd.name.toString, coerced)
      }
  }
}
