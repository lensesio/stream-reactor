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
package io.lenses.streamreactor.connect.cloud.common.formats.reader.converters

import io.lenses.streamreactor.connect.cloud.common.formats.reader.Converter
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.source.SourceWatermark
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.header.ConnectHeaders
import org.apache.kafka.connect.header.Headers
import org.apache.kafka.connect.source.SourceRecord

import java.time.Instant
import scala.jdk.CollectionConverters.ListHasAsScala

/**
  * It expects the payload to be the envelope:
  *
  * {{{
  *  {
  *    "key": ...
  *    "value": ...
  *    "headers": {
  *    "header1": "value1",
  *    "header2": "value2"
  *    }
  *    metadata: {
  *    "timestamp": 1234567890,
  *    "topic": "my-topic",
  *    "partition": 0,
  *    "offset": 1234567890
  *    }
  *  }
  * }}}
  *
  * It will extract the key, value, headers and metadata and create a SourceRecord with the key, value and headers.
  * The metadata will be used to set the timestamp and target partition.
  *
  * The key, value, headers and metadata are expected to be optional. If they are missing it will set the value to null.
  * @param watermarkPartition The watermark partition
  * @param topic The target topic
  * @param partition The target partition; only used if the envelope does not contain a partition
  * @param s3Location The S3 location of the object
  * @param lastModified The last modified date of the object
  */
class SchemaAndValueEnvelopeConverter(
  watermarkPartition: java.util.Map[String, String],
  topic:              Topic,
  partition:          Integer,
  s3Location:         CloudLocation,
  lastModified:       Instant,
  instantF:           () => Instant = () => Instant.now(),
) extends Converter[SchemaAndValue] {
  override def convert(schemaAndValue: SchemaAndValue, index: Long): SourceRecord = {
    if (schemaAndValue.schema().`type`() != Schema.Type.STRUCT) {
      throw new RuntimeException(
        s"Invalid schema type [${schemaAndValue.schema().`type`()}]. Expected [${Schema.Type.STRUCT}]",
      )
    }
    val struct = schemaAndValue.value().asInstanceOf[org.apache.kafka.connect.data.Struct]
    val fields = struct.schema().fields().asScala.map(_.name()).toSet

    var key:       Any    = null
    var keySchema: Schema = null
    if (fields.contains("key")) {
      key       = struct.get("key")
      keySchema = struct.schema().field("key").schema()
    }

    var value:       Any    = null
    var valueSchema: Schema = null
    if (fields.contains("value")) {
      value       = struct.get("value")
      valueSchema = struct.schema().field("value").schema()
    }

    var headers: Headers = null
    if (fields.contains("headers")) {
      headers = new ConnectHeaders()
      struct.get("headers").asInstanceOf[org.apache.kafka.connect.data.Struct].schema().fields().asScala
        .foldLeft(headers) {
          case (headers, field) =>
            val header = struct.get("headers").asInstanceOf[org.apache.kafka.connect.data.Struct].get(field)
            headers.add(field.name(), header, field.schema())
        }
    }

    val partition = if (fields.contains("metadata")) {
      struct.get("metadata").asInstanceOf[org.apache.kafka.connect.data.Struct].getInt32("partition")
    } else this.partition

    val timestamp: Long = if (fields.contains("metadata")) {
      struct.get("metadata").asInstanceOf[org.apache.kafka.connect.data.Struct].getInt64("timestamp")
    } else instantF().toEpochMilli

    new SourceRecord(
      watermarkPartition,
      SourceWatermark.offset(s3Location, index, lastModified),
      topic.value,
      partition,
      keySchema,
      key,
      valueSchema,
      value,
      timestamp,
      headers,
    )
  }
}
