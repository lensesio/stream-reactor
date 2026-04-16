/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.http.sink.tpl.renderer

import cats.data.NonEmptySeq
import cats.implicits._
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.http.sink.config.NullPayloadHandler
import io.lenses.streamreactor.connect.http.sink.tpl.substitutions.SubstitutionError
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord
import io.lenses.streamreactor.connect.http.sink.tpl.substitutions.SubstitutionType
import org.apache.kafka.connect.sink.SinkRecord
import io.lenses.streamreactor.connect.http.sink.tpl.Headers
import scala.jdk.CollectionConverters._
import io.lenses.streamreactor.connect.http.sink.tpl.substitutions.HeaderValueConverter

object RecordRenderer {

  private val templateRenderer = new TemplateRenderer[SubstitutionType](SubstitutionType)

  def renderRecords(
    data:               NonEmptySeq[SinkRecord],
    endpointTpl:        String,
    contentTpl:         String,
    headers:            Headers,
    nullPayloadHandler: NullPayloadHandler,
  ): Either[SubstitutionError, NonEmptySeq[RenderedRecord]] =
    data.map(renderRecord(_, endpointTpl, contentTpl, headers, nullPayloadHandler)).sequence
  def renderRecord(
    sinkRecord:         SinkRecord,
    endpointTpl:        String,
    contentTpl:         String,
    headers:            Headers,
    nullPayloadHandler: NullPayloadHandler,
  ): Either[SubstitutionError, RenderedRecord] = {
    val topicPartitionOffset: TopicPartitionOffset =
      Topic(sinkRecord.topic()).withPartition(sinkRecord.kafkaPartition()).withOffset(Offset(sinkRecord.kafkaOffset()))

    for {
      recordRend:   String                <- templateRenderer.render(sinkRecord, contentTpl, nullPayloadHandler)
      headersRend:  Seq[(String, String)] <- renderHeaders(sinkRecord, headers, nullPayloadHandler)
      endpointRend: String                <- templateRenderer.render(sinkRecord, endpointTpl, nullPayloadHandler)
    } yield RenderedRecord(topicPartitionOffset, sinkRecord.timestamp(), recordRend, headersRend, endpointRend)
  }

  private def renderHeader(
    sinkRecord:         SinkRecord,
    header:             (String, String),
    nullPayloadHandler: NullPayloadHandler,
  ): Either[SubstitutionError, (String, String)] =
    header match {
      case (hKey, hVal) =>
        for {
          k <- templateRenderer.render(sinkRecord, hKey, nullPayloadHandler)
          v <- templateRenderer.render(sinkRecord, hVal, nullPayloadHandler)
        } yield k -> v
    }

  private def renderTemplateHeaders(
    sinkRecord:         SinkRecord,
    headers:            Headers,
    nullPayloadHandler: NullPayloadHandler,
  ): Either[SubstitutionError, Seq[(String, String)]] =
    headers.headerTemplates.map(h => renderHeader(sinkRecord, h, nullPayloadHandler)).sequence

  private def extractMessageHeaders(
    sinkRecord:         SinkRecord,
    copyMessageHeaders: Boolean,
  ): Seq[(String, String)] =
    if (copyMessageHeaders && Option(sinkRecord.headers()).isDefined)
      sinkRecord.headers().iterator().asScala.flatMap { h =>
        (Option(h.value()), Option(h.schema())) match {
          case (Some(value), Some(schema)) =>
            Some(h.key() -> HeaderValueConverter.headerValueToString(value, schema))
          case (Some(value), None) =>
            Some(h.key() -> value.toString)
          case _ => None
        }
      }.toSeq
    else Seq.empty

  private def combineHeaders(
    templateHeaders: Seq[(String, String)],
    messageHeaders:  Seq[(String, String)],
  ): Seq[(String, String)] =
    // Message headers first, then template headers, so template headers take precedence
    (messageHeaders ++ templateHeaders).toMap.toSeq

  private def renderHeaders(
    sinkRecord:         SinkRecord,
    headers:            Headers,
    nullPayloadHandler: NullPayloadHandler,
  ): Either[SubstitutionError, Seq[(String, String)]] =
    for {
      templateHeaders <- renderTemplateHeaders(sinkRecord, headers, nullPayloadHandler)
      messageHeaders   = extractMessageHeaders(sinkRecord, headers.copyMessageHeaders)
    } yield combineHeaders(templateHeaders, messageHeaders)

}
