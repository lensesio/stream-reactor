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

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.kafka.connect.data.Struct
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema.MessageType

class StructReadSupport extends ReadSupport[Struct] {

  override def prepareForRead(
    configuration: Configuration,
    metaData:      util.Map[String, String],
    fileSchema:    MessageType,
    context:       ReadSupport.ReadContext,
  ): RecordMaterializer[Struct] = {
    // the file schema in here comes from the footer of the parquet file
    val schema = ParquetSchemas.toKafka(fileSchema)
    new StructMaterializer(schema)
  }

  override def init(context: InitContext): ReadSupport.ReadContext =
    new ReadSupport.ReadContext(context.getFileSchema)
}
