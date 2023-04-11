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
package com.landoop.streamreactor.connect.hive.sink.mapper

import cats.data.NonEmptyList
import com.datamountaineer.kcql.Field
import com.landoop.streamreactor.connect.hive.StructMapper
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.ConnectException

/**
  * compile of [[StructMapper]] that will apply
  * a KCQL projection - dropping fields not specified in the
  * projection and mapping fields to their aliases.
  */
class ProjectionMapper(projection: NonEmptyList[Field]) extends StructMapper {

  override def map(input: Struct): Struct = {
    // the compatible output schema built from projected fields with aliases applied
    val builder = projection.foldLeft(SchemaBuilder.struct) { (builder, kcqlField) =>
      Option(input.schema.field(kcqlField.getName)).fold(throw new ConnectException(s"Missing field $kcqlField")) {
        field =>
          builder.field(kcqlField.getAlias, field.schema)
      }
    }
    val schema = builder.build()
    projection.foldLeft(new Struct(schema)) { (struct, field) =>
      struct.put(field.getAlias, input.get(field.getName))
    }
  }
}
