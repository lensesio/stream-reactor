/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.redis.sink.rowkeys

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord

import scala.jdk.CollectionConverters.ListHasAsScala

/**
  * Builds a new key from the payload fields specified
  *
  * @param keys The key to build
  * @param keyDelimiter Row key delimiter
  */
case class StringStructFieldsStringKeyBuilder(keys: Seq[String], keyDelimiter: String = ".") extends StringKeyBuilder {
  private val availableSchemaTypes = Set(
    Schema.Type.BOOLEAN,
    Schema.Type.BYTES,
    Schema.Type.FLOAT32,
    Schema.Type.FLOAT64,
    Schema.Type.INT8,
    Schema.Type.INT16,
    Schema.Type.INT32,
    Schema.Type.INT64,
    Schema.Type.STRING,
  )

  require(keys.nonEmpty, "Invalid keys provided")

  /**
    * Builds a row key for a records
    *
    * @param record a SinkRecord to build the key for
    * @return A row key string
    */
  override def build(record: SinkRecord): String = {
    val struct = record.value().asInstanceOf[Struct]
    val schema = struct.schema

    val availableFields = schema.fields().asScala.map(_.name).toSet
    val missingKeys     = keys.filterNot(availableFields.contains)
    require(
      missingKeys.isEmpty,
      s"[${missingKeys.mkString(",")}] keys are not present in the SinkRecord payload: [${availableFields.mkString(",")}]",
    )

    keys.flatMap { key =>
      val field = schema.field(key)
      val value = struct.get(field)

      require(value != null,
              s"[$key] field value is null. Non null value is required for the fields creating the Hbase row key",
      )
      if (availableSchemaTypes.contains(field.schema().`type`())) Some(value.toString)
      else None
    }.mkString(keyDelimiter)
  }
}
