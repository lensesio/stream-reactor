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

import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.parquet.io.api.Converter
import org.apache.parquet.io.api.GroupConverter

import scala.jdk.CollectionConverters.ListHasAsScala

class RootGroupConverter(schema: Schema) extends GroupConverter with StrictLogging {
  require(schema.`type`() == Schema.Type.STRUCT)

  var struct: Struct = _
  private val builder    = scala.collection.mutable.Map.empty[String, Any]
  private val converters = schema.fields.asScala.map(Converters.get(_, builder)).toIndexedSeq

  override def getConverter(k: Int): Converter = converters(k)
  override def start(): Unit = builder.clear()
  override def end(): Unit = struct = {
    val struct = new Struct(schema)
    schema.fields.asScala.map { field =>
      val value = builder.getOrElse(field.name, null)
      try {
        struct.put(field, value)
      } catch {
        case t: Exception =>
          throw t
      }
    }
    struct
  }
}
