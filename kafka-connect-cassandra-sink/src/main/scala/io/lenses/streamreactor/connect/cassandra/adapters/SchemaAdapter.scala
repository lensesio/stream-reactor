/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cassandra.adapters

import com.datastax.oss.common.sink.AbstractField
import com.datastax.oss.common.sink.AbstractSchema
import io.lenses.streamreactor.connect.cassandra.adapters.SchemaAdapter.convert
import org.apache.kafka.connect.data.Schema

import java.util

case class SchemaAdapter(schema: Schema) extends AbstractSchema {

  private val tpe = convert(schema.`type`())
  private val fieldWrappers: util.List[FieldAdapter] = {
    if (schema.`type`() != Schema.Type.STRUCT) {
      util.Collections.emptyList[FieldAdapter]()
    } else {
      schema.fields().stream()
        .map(f => FieldAdapter(f))
        .collect(util.stream.Collectors.toList[FieldAdapter]())
    }
  }
  override def valueSchema(): AbstractSchema = SchemaAdapter.from(schema.valueSchema())

  override def keySchema(): AbstractSchema = SchemaAdapter.from(schema.keySchema())

  override def `type`(): AbstractSchema.Type = tpe

  override def fields(): util.List[_ <: AbstractField] = fieldWrappers

  override def field(name: String): AbstractField =
    fieldWrappers.stream()
      .filter(f => f.name() == name)
      .findFirst()
      .orElse(null)
}

object SchemaAdapter {
  private val INT8 = new SchemaAdapter(Schema.INT8_SCHEMA)

  private val INT16   = new SchemaAdapter(Schema.INT16_SCHEMA)
  private val INT32   = new SchemaAdapter(Schema.INT32_SCHEMA)
  private val INT64   = new SchemaAdapter(Schema.INT64_SCHEMA)
  private val FLOAT32 = new SchemaAdapter(Schema.FLOAT32_SCHEMA)
  private val FLOAT64 = new SchemaAdapter(Schema.FLOAT64_SCHEMA)
  private val BOOLEAN = new SchemaAdapter(Schema.BOOLEAN_SCHEMA)
  private val STRING  = new SchemaAdapter(Schema.STRING_SCHEMA)
  private val BYTES   = new SchemaAdapter(Schema.BYTES_SCHEMA)

  private val schemasMap = Map(
    Schema.INT8_SCHEMA    -> INT8,
    Schema.INT16_SCHEMA   -> INT16,
    Schema.INT32_SCHEMA   -> INT32,
    Schema.INT64_SCHEMA   -> INT64,
    Schema.FLOAT32_SCHEMA -> FLOAT32,
    Schema.FLOAT64_SCHEMA -> FLOAT64,
    Schema.BOOLEAN_SCHEMA -> BOOLEAN,
    Schema.STRING_SCHEMA  -> STRING,
    Schema.BYTES_SCHEMA   -> BYTES,
  )

  def from(schema: Schema): SchemaAdapter =
    schemasMap.getOrElse(schema, new SchemaAdapter(schema))

  def convert(`type`: Schema.Type): AbstractSchema.Type = `type` match {
    case Schema.Type.INT8    => AbstractSchema.Type.INT8
    case Schema.Type.INT16   => AbstractSchema.Type.INT16
    case Schema.Type.INT32   => AbstractSchema.Type.INT32
    case Schema.Type.INT64   => AbstractSchema.Type.INT64
    case Schema.Type.FLOAT32 => AbstractSchema.Type.FLOAT32
    case Schema.Type.FLOAT64 => AbstractSchema.Type.FLOAT64
    case Schema.Type.BOOLEAN => AbstractSchema.Type.BOOLEAN
    case Schema.Type.STRING  => AbstractSchema.Type.STRING
    case Schema.Type.BYTES   => AbstractSchema.Type.BYTES
    case Schema.Type.ARRAY   => AbstractSchema.Type.ARRAY
    case Schema.Type.MAP     => AbstractSchema.Type.MAP
    case Schema.Type.STRUCT  => AbstractSchema.Type.STRUCT
    case other               => throw new IllegalArgumentException(s"Cannot convert type $other to Cassandra type")
  }
}
