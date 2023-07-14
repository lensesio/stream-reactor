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
package io.lenses.streamreactor.connect.aws.s3.formats.writer

import io.lenses.streamreactor.connect.aws.s3.formats.writer.MessageDetail.{MetadataSchema, headersSchema}
import io.lenses.streamreactor.connect.aws.s3.model.Topic
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder

import java.time.Instant

case class MessageDetail(
  keySinkData:   Option[SinkData],
  valueSinkData: SinkData,
  headers:       Map[String, SinkData],
  time:          Option[Instant],
  topic:         Topic,
  partition:     Int,
) {

  /**
    * Creates an envelope schema for the message detail. This is a schema that contains the key, value and headers and metadata
    * {{{
    *   {
    *     "key": ...,
    *     "value": ...,
    *     "headers": {
    *       "header1": "value1",
    *       "header2": "value2"
    *     },
    *     metadata: {
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
  def envelopeSchema: Schema = {
    //create a connect schema
    var builder = SchemaBuilder.struct()
      .field("metadata", MetadataSchema)

    builder = keySinkData.flatMap(_.schema()).fold(builder)(builder.field("key", _))
    builder = valueSinkData.schema().fold(builder)(builder.field("value", _))
    builder = headersSchema(headers).fold(builder)(builder.field("headers", _))
    builder.build()
  }
}

object MessageDetail {
  val MetadataSchema: Schema = SchemaBuilder.struct().field("timestamp", Schema.INT64_SCHEMA)
    .field("topic", Schema.STRING_SCHEMA)
    .field("partition", Schema.INT32_SCHEMA)
    .field("offset", Schema.INT64_SCHEMA)
    .build()

  private val DefaultHeadersSchema: SchemaBuilder = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA)
  def headersSchema(headers: Map[String, SinkData]): Option[Schema] = {
    val headersList = headers.filter(_._2.schema().isDefined).toList.sortBy(_._1)
    headersList.headOption.map { _ =>
      val builder = headersList.foldLeft(SchemaBuilder.struct()) {
        case (builder, (key, data)) =>
          builder.field(key, data.schema().get)
      }
      builder.build()
    }
  }
}
