/*
 *
 *  * Copyright 2020 Lenses.io.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.datamountaineer.streamreactor.common.rowkeys

import org.apache.kafka.connect.data.{Schema, Struct}
import org.apache.kafka.connect.sink.SinkRecord

import scala.jdk.CollectionConverters.ListHasAsScala

/**
  * Builds the new record key for the given connect SinkRecord.
  */
trait StringKeyBuilder {
  def build(record: SinkRecord): String
}

/**
  * Uses the connect record (topic, partition, offset) to set the schema
  *
  * @param keyDelimiter Row key delimiter
  * @return a unique string for the message identified by: <topic>|<partition>|<offset> 
  */
class StringGenericRowKeyBuilder(keyDelimiter: String = "|") extends StringKeyBuilder {

  override def build(record: SinkRecord): String = {
    Seq(record.topic(), record.kafkaPartition(), record.kafkaOffset().toString).mkString(keyDelimiter)
  }
}

/**
  * Creates a key based on the connect SinkRecord instance key. Only connect Schema primitive types are handled
  */
class StringSinkRecordKeyBuilder extends StringKeyBuilder {
  override def build(record: SinkRecord): String = {
    val `type` = record.keySchema().`type`()
    require(`type`.isPrimitive, "The SinkRecord key schema is not a primitive type")

    `type`.name() match {
      case "INT8" | "INT16" | "INT32" | "INT64" | "FLOAT32" | "FLOAT64" | "BOOLEAN" | "STRING" | "BYTES" => record.key().toString
      case other => throw new IllegalArgumentException(s"$other is not supported by the ${getClass.getName}")
    }
  }
}

/**
  * Builds a new key from the payload fields specified
  *
  * @param keys The key to build
  * @param keyDelimiter Row key delimiter
  */
case class StringStructFieldsStringKeyBuilder(keys: Seq[String],
                                              keyDelimiter: String = ".") extends StringKeyBuilder {
  private val availableSchemaTypes = Set(
    Schema.Type.BOOLEAN,
    Schema.Type.BYTES,
    Schema.Type.FLOAT32,
    Schema.Type.FLOAT64,
    Schema.Type.INT8,
    Schema.Type.INT16,
    Schema.Type.INT32,
    Schema.Type.INT64,
    Schema.Type.STRING
  )

  require(keys.nonEmpty, "Invalid keys provided")

  /**
    * Builds a row key for a records
    *
    * @param record a SinkRecord to build the key for
    * @return A row key string
    * */
  override def build(record: SinkRecord): String = {
    val struct = record.value().asInstanceOf[Struct]
    val schema = struct.schema

    val availableFields = schema.fields().asScala.map(_.name).toSet
    val missingKeys = keys.filterNot(availableFields.contains)
    require(missingKeys.isEmpty, s"[${missingKeys.mkString(",")}] keys are not present in the SinkRecord payload: [${availableFields.mkString(",")}]")

    keys.flatMap { key =>
      val field = schema.field(key)
      val value = struct.get(field)

      require(value != null, s"[$key] field value is null. Non null value is required for the fields creating the Hbase row key")
      if (availableSchemaTypes.contains(field.schema().`type`())) Some(value.toString)
      else None
    }.mkString(keyDelimiter)
  }
}
