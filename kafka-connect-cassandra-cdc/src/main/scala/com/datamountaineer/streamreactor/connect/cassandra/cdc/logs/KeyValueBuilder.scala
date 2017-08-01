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
import com.datamountaineer.streamreactor.connect.cassandra.cdc.metadata.{ChangeStructBuilder, ConnectSchemaBuilder, SubscriptionDataProvider}
import org.apache.cassandra.config.CFMetaData
import org.apache.cassandra.db.partitions.PartitionUpdate
import org.apache.kafka.connect.data.Struct

import scala.collection.JavaConversions._

object KeyValueBuilder {
  def apply(cf: CFMetaData, pu: PartitionUpdate)(implicit dataProvider: SubscriptionDataProvider, config: CdcConfig): Struct = {
    val schema = dataProvider.getChangeSchema(cf.ksName, cf.cfName)
      .getOrElse(throw new IllegalArgumentException(s"Cannot find '${cf.ksName}.${cf.cfName}' schema for Connect Source Record Key."))

    val struct = new Struct(schema)
    struct.put(ChangeStructBuilder.KeyspaceField, cf.ksName)
    struct.put(ChangeStructBuilder.TableField, cf.cfName)

    val pkSchema = schema.field(ChangeStructBuilder.KeysField).schema()
    val keysStruct = new Struct(pkSchema)
    cf.partitionKeyColumns().foreach { cd =>
      val keyValue = cd.cellValueType().getSerializer.deserialize(pu.partitionKey().getKey)
      val coercedValue = ConnectSchemaBuilder.coerceValue(keyValue, cd.cellValueType(), pkSchema.field(cd.name.toString).schema())
      keysStruct.put(cd.name.toString, coercedValue)
      //pu.partitionKey()
    }
   /* cf.primaryKeyColumns()
      .map { cd =>
        val value = cd.cellValueType().getSerializer.deserialize(pu.partitionKey().getKey)
        keysStruct.put(cd.name.toString, value)
      }*/

    struct.put(ChangeStructBuilder.KeysField, keysStruct)
    struct.put(ChangeStructBuilder.TimestampField, pu.maxTimestamp())
    struct
  }
}
