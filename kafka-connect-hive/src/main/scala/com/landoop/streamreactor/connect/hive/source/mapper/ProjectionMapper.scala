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

import cats.data.NonEmptyList
import com.landoop.streamreactor.connect.hive.StructMapper
import com.landoop.streamreactor.connect.hive.source.config.ProjectionField
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.ConnectException

class ProjectionMapper(projection: NonEmptyList[ProjectionField]) extends StructMapper {

  override def map(input: Struct): Struct = {
    val builder = projection.foldLeft(SchemaBuilder.struct) { (builder, projectionField) =>
      Option(input.schema.field(projectionField.name))
        .fold(throw new ConnectException(s"Projection field ${projectionField.name} cannot be found in input")) {
          field =>
            builder.field(projectionField.alias, field.schema)
        }
    }
    val schema = builder.build()
    projection.foldLeft(new Struct(schema)) { (struct, field) =>
      struct.put(field.alias, input.get(field.name))
    }
  }
}
