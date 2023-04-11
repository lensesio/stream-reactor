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

import com.landoop.streamreactor.connect.hive._
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema
import org.apache.parquet.io.api.Converter

object Converters {
  def get(field: Field, builder: scala.collection.mutable.Map[String, Any]): Converter =
    field.schema().`type`() match {
      case Schema.Type.STRUCT => new NestedGroupConverter(field.schema(), field, builder)
      case Schema.Type.INT64 | Schema.Type.INT32 | Schema.Type.INT16 | Schema.Type.INT8 =>
        new AppendingPrimitiveConverter(field, builder)
      case Schema.Type.FLOAT64 | Schema.Type.FLOAT32 => new AppendingPrimitiveConverter(field, builder)
      // case Schema.Type.INT64 => new TimestampPrimitiveConverter(field, builder)
      case Schema.Type.STRING => new DictionaryStringPrimitiveConverter(field, builder)
      case Schema.Type.ARRAY  => ???
      case other              => throw UnsupportedSchemaType(s"Unsupported data type $other")
    }
}
