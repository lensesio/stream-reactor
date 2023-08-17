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
package io.lenses.streamreactor.connect.aws.s3.sink.transformers

import io.lenses.streamreactor.connect.aws.s3.config.DataStorageSettings
import io.lenses.streamreactor.connect.aws.s3.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.aws.s3.formats.writer.SinkData
import io.lenses.streamreactor.connect.aws.s3.formats.writer.StructSinkData
import io.lenses.streamreactor.connect.aws.s3.model.Topic
import io.lenses.streamreactor.connect.aws.s3.sink.transformers.MessageTransformer.envelope
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct

class MessageTransformer(storageSettingsMap: Map[Topic, DataStorageSettings]) {
  def transform(message: MessageDetail): MessageDetail =
    storageSettingsMap.get(message.topic).fold(message) { settings =>
      if (settings.isDataStored) {
        envelope(message, settings)
      } else {
        message
      }

    }
}

object MessageTransformer {
  private val MetadataSchema: Schema = SchemaBuilder.struct()
    .field("timestamp", Schema.INT64_SCHEMA).optional()
    .field("topic", Schema.STRING_SCHEMA)
    .field("partition", Schema.INT32_SCHEMA)
    .field("offset", Schema.INT64_SCHEMA)
    .build()

  /**
    * Creates an envelope schema for the message detail. This is a schema that contains the key, value and headers and metadata.
    * Key and Value schema is set optional to handle null data (i.e. deletes as tombstones)
    * {{{
    *   {
    *     "key": ...,
    *     "value": ...,
    *     "headers": {
    *       "header1": "value1",
    *       "header2": "value2"
    *     },
    *     "metadata": {
    *       "timestamp": 123456789,
    *       "topic": "topic1",
    *       "partition": 0,
    *       "offset": 1
    *
    *     }
    *   }
    * }}}
    *
    * @return
    */
  private def envelope(message: MessageDetail, settings: DataStorageSettings): MessageDetail = {
    val schema   = envelopeSchema(message, settings)
    val envelope = new Struct(schema)
    envelope.put("value", message.value.value)
    if (settings.metadata) envelope.put("metadata", metadataData(message))
    if (settings.key) message.key.foreach(k => envelope.put("key", k.value))
    if (settings.headers) {
      Option(schema.field("headers")).foreach { field =>
        envelope.put("headers", headersData(message, field.schema()))
      }
    }
    message.copy(value = StructSinkData(envelope))
  }

  private def metadataData(message: MessageDetail): Struct = {
    val metadata = new Struct(MetadataSchema)
    metadata.put("timestamp", message.time.orNull)
    metadata.put("topic", message.topic)
    metadata.put("partition", message.partition)
    metadata.put("offset", message.offset)
    metadata
  }
  def envelopeSchema(message: MessageDetail, settings: DataStorageSettings): Schema = {
    var builder: SchemaBuilder = SchemaBuilder.struct()

    builder =
      if (settings.key) message.key.flatMap(_.schema()).fold(builder)(builder.field("key", _).optional())
      else builder
    //set the value schema optional to handle delete records
    builder = message.value.schema().fold(builder)(builder.field("value", _).optional())
    builder =
      if (settings.headers) headersSchema(message.headers).fold(builder)(builder.field("headers", _))
      else builder
    builder = if (settings.metadata) builder.field("metadata", MetadataSchema) else builder
    builder.build()
  }

  private def headersSchema(headers: Map[String, SinkData]): Option[Schema] = {
    val headersList = headers.filter(_._2.schema().isDefined).toList.sortBy(_._1)
    headersList.headOption.map { _ =>
      val builder = headersList.foldLeft(SchemaBuilder.struct()) {
        case (builder, (key, data)) =>
          builder.field(key, data.schema().get)
      }
      builder.build()
    }
  }

  private def headersData(message: MessageDetail, schema: Schema): Struct = {
    val struct = new Struct(schema)
    message.headers.foreach {
      case (key, data) =>
        struct.put(key, data.value)
    }
    struct
  }

}
