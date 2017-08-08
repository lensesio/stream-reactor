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

import com.datamountaineer.streamreactor.connect.cassandra.cdc.metadata.ConnectSchemaBuilder.MetadataSchema
import org.apache.kafka.connect.data.{SchemaBuilder, Struct}

/**
  * Builds the Struct for the mutation. The struct has two fields:
  * 1. metadata
  * a: changeType
  * b: deleted_columns
  * c: timestamp
  * 2. cdc
  * all PK, Clustering and (non PK, non-clustering) Fields Changed
  */
object ValueStructBuilder {
  val MetadataField = "metadata"
  val CdcField = "cdc"

  def apply(cdc: Struct, metadata: Struct): Struct = {

    val changeBuilder = SchemaBuilder.struct().name("mutation")
    changeBuilder.field(MetadataField, MetadataSchema)
    changeBuilder.field(CdcField, cdc.schema())
    val valueSchema = changeBuilder.build()


    val struct = new Struct(valueSchema)
    struct.put(MetadataField, metadata)
    struct.put(CdcField, cdc)

    struct
  }
}
