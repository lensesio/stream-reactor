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
package com.landoop.streamreactor.connect.hive.parquet

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.parquet.io.api.GroupConverter
import org.apache.parquet.io.api.RecordMaterializer

/**
  * Top level class used to serialize objects from a stream of Parquet data.
  *
  * Each record will be wrapped by {@link GroupConverter#start()} and {@link GroupConverter#end()},
  * between which the appropriate fields will be materialized.
  */
class StructMaterializer(schema: Schema) extends RecordMaterializer[Struct] {
  private val root = new RootGroupConverter(schema)
  override def getRootConverter: GroupConverter = root
  override def getCurrentRecord: Struct         = root.struct
}
