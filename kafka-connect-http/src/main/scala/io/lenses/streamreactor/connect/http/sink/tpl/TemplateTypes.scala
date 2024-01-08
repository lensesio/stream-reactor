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
package io.lenses.streamreactor.connect.http.sink.tpl

import cats.implicits.catsSyntaxEitherId
import cats.implicits.catsSyntaxOptionId
import cats.implicits.toTraverseOps
import io.lenses.streamreactor.connect.http.sink.tpl.renderer.RecordRenderer
import io.lenses.streamreactor.connect.http.sink.tpl.substitutions.SubstitutionError
import org.apache.kafka.connect.sink.SinkRecord

object RawTemplate {
  private val innerTemplatePattern = """\{\{#message}}([\s\S]*?)\{\{/message}}""".r

  def apply(endpoint: String, content: String, headers: Seq[(String, String)]): TemplateType =
    innerTemplatePattern.findFirstMatchIn(content) match {
      case Some(innerTemplate) =>
        val start = content.substring(0, innerTemplate.start)
        val end   = content.substring(innerTemplate.end)
        TemplateWithInnerLoop(endpoint, start, end, innerTemplate.group(1), headers)
      case None =>
        SimpleTemplate(endpoint, content, headers)
    }
}

trait TemplateType {
  def endpoint: String
  def headers:  Seq[(String, String)]

  def renderRecords(record: Seq[SinkRecord]): Either[SubstitutionError, Seq[RenderedRecord]]

  def process(records: Seq[RenderedRecord]): Either[SubstitutionError, ProcessedTemplate]
}

// this template type will require individual requests, the messages can't be batched
case class SimpleTemplate(
  endpoint: String,
  content:  String,
  headers:  Seq[(String, String)],
) extends TemplateType {

  override def renderRecords(records: Seq[SinkRecord]): Either[SubstitutionError, Seq[RenderedRecord]] =
    RecordRenderer.renderRecords(records, endpoint.some, content, headers)

  override def process(records: Seq[RenderedRecord]): Either[SubstitutionError, ProcessedTemplate] =
    records.headOption match {
      case Some(RenderedRecord(_, recordRendered, headersRendered, Some(endpointRendered))) =>
        ProcessedTemplate(endpointRendered, recordRendered, headersRendered).asRight
      case _ => SubstitutionError("No record found").asLeft
    }
}

case class TemplateWithInnerLoop(
  endpoint:      String,
  prefixContent: String,
  suffixContent: String,
  innerTemplate: String,
  headers:       Seq[(String, String)],
) extends TemplateType {

  override def renderRecords(records: Seq[SinkRecord]): Either[SubstitutionError, Seq[RenderedRecord]] =
    records.zipWithIndex.map {
      case (record, i) =>
        RecordRenderer.renderRecord(
          record,
          Option.when(i == 0)(endpoint),
          innerTemplate,
          headers,
        )
    }.sequence

  override def process(records: Seq[RenderedRecord]): Either[SubstitutionError, ProcessedTemplate] = {

    val replaceWith    = records.flatMap(_.recordRendered).mkString("")
    val contentOrError = prefixContent + replaceWith + suffixContent
    val maybeProcessedTpl = for {
      headRecord <- records.headOption
      ep         <- headRecord.endpointRendered
    } yield {
      ProcessedTemplate(
        endpoint = ep,
        content  = contentOrError,
        headers  = records.flatMap(_.headersRendered).distinct,
      )
    }
    maybeProcessedTpl.toRight(SubstitutionError("No record or endpoint available"))
  }

}
