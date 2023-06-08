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
package com.datamountaineer.streamreactor.common.converters

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.storage.Converter

import java.util

/**
  * Pass-through converter for raw byte data.
  */
class ByteArrayConverter extends Converter {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
  override def fromConnectData(topic: String, schema: Schema, value: AnyRef): Array[Byte] = {
    if (schema != null && (schema.`type` ne Schema.Type.BYTES) && !(schema == Schema.OPTIONAL_BYTES_SCHEMA))
      throw new DataException("Invalid schema type for ByteArrayConverter: " + schema.`type`.toString)
    if (value != null && !value.isInstanceOf[Array[Byte]])
      throw new DataException("ByteArrayConverter is not compatible with objects of type " + value.getClass)
    value.asInstanceOf[Array[Byte]]
  }
  override def toConnectData(topic: String, value: Array[Byte]) =
    new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, value)
}
