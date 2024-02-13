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
package io.lenses.streamreactor.connect.http.sink.tpl.renderer

import cats.implicits._
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.http.sink.tpl.substitutions.SubstitutionError
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord
import org.apache.kafka.connect.sink.SinkRecord

object RecordRenderer {

  def renderRecords(
    data:        Seq[SinkRecord],
    endpointTpl: Option[String],
    contentTpl:  String,
    headers:     Seq[(String, String)],
  ): Either[SubstitutionError, Seq[RenderedRecord]] =
    data.map(renderRecord(_, endpointTpl, contentTpl, headers)).sequence
  def renderRecord(
    sinkRecord:  SinkRecord,
    endpointTpl: Option[String],
    contentTpl:  String,
    headers:     Seq[(String, String)],
  ): Either[SubstitutionError, RenderedRecord] = {
    val topicPartitionOffset: TopicPartitionOffset =
      Topic(sinkRecord.topic()).withPartition(sinkRecord.kafkaPartition()).withOffset(Offset(sinkRecord.kafkaOffset()))

    for {
      recordRend:   String <- TemplateRenderer.render(sinkRecord, contentTpl)
      headersRend:  Seq[(String, String)] <- renderHeaders(sinkRecord, headers)
      endpointRend: Option[String] <- renderEndpoint(sinkRecord, endpointTpl)
    } yield RenderedRecord(topicPartitionOffset, recordRend, headersRend, endpointRend)
  }

  private def renderHeader(
    sinkRecord: SinkRecord,
    header:     (String, String),
  ): Either[SubstitutionError, (String, String)] =
    header match {
      case (hKey, hVal) =>
        for {
          k <- TemplateRenderer.render(sinkRecord, hKey)
          v <- TemplateRenderer.render(sinkRecord, hVal)
        } yield k -> v
    }

  private def renderHeaders(
    sinkRecord: SinkRecord,
    headers:    Seq[(String, String)],
  ): Either[SubstitutionError, Seq[(String, String)]] =
    headers.map(h => renderHeader(sinkRecord, h)).sequence

  private def renderEndpoint(
    sinkRecord:  SinkRecord,
    endpointTpl: Option[String],
  ): Either[SubstitutionError, Option[String]] =
    endpointTpl.map(tpl => TemplateRenderer.render(sinkRecord, tpl)).sequence

}
