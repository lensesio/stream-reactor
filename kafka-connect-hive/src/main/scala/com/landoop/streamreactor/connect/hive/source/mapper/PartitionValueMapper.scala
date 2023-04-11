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
package com.landoop.streamreactor.connect.hive.source.mapper

import com.landoop.streamreactor.connect.hive.Partition
import com.landoop.streamreactor.connect.hive.StructMapper
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct

import scala.jdk.CollectionConverters.ListHasAsScala

class PartitionValueMapper(partition: Partition) extends StructMapper {
  override def map(input: Struct): Struct = {

    val builder = SchemaBuilder.struct()
    input.schema.fields.asScala.foreach { field =>
      builder.field(field.name, field.schema)
    }
    partition.entries.toList.foreach { entry =>
      builder.field(entry._1.value, Schema.STRING_SCHEMA)
    }
    val schema = builder.build()

    val struct = new Struct(schema)
    input.schema.fields.asScala.foreach { field =>
      struct.put(field.name, input.get(field.name))
    }
    partition.entries.toList.foreach { entry =>
      struct.put(entry._1.value, entry._2)
    }
    struct
  }
}
