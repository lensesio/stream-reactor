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

import org.apache.kafka.connect.sink.SinkRecord

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

  override def build(record: SinkRecord): String =
    Seq(record.topic(), record.kafkaPartition(), record.kafkaOffset().toString).mkString(keyDelimiter)
}

/**
  * Creates a key based on the connect SinkRecord instance key. Only connect Schema primitive types are handled
  */
class StringSinkRecordKeyBuilder extends StringKeyBuilder {
  override def build(record: SinkRecord): String = {
    val `type` = record.keySchema().`type`()
    require(`type`.isPrimitive, "The SinkRecord key schema is not a primitive type")

    `type`.name() match {
      case "INT8" | "INT16" | "INT32" | "INT64" | "FLOAT32" | "FLOAT64" | "BOOLEAN" | "STRING" | "BYTES" =>
        record.key().toString
      case other => throw new IllegalArgumentException(s"$other is not supported by the ${getClass.getName}")
    }
  }
}
