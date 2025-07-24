/*
 * Copyright 2017-2025 Lenses.io Ltd
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

import cats.data.NonEmptySeq
import cats.implicits.catsSyntaxEitherId
import cats.implicits.toTraverseOps
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.http.sink.config.NullPayloadHandler
import io.lenses.streamreactor.connect.http.sink.tpl.JsonTidy.cleanUp
import io.lenses.streamreactor.connect.http.sink.tpl.renderer.RecordRenderer
import io.lenses.streamreactor.connect.http.sink.tpl.substitutions.SubstitutionError
import org.apache.kafka.connect.sink.SinkRecord

case class Headers(
  headerTemplates:    Seq[(String, String)],
  copyMessageHeaders: Boolean,
)

object RawTemplate {
  private val innerTemplatePattern = """\{\{#message}}([\s\S]*?)\{\{/message}}""".r

  def apply(
    endpoint:           String,
    content:            String,
    headers:            Headers,
    nullPayloadHandler: NullPayloadHandler,
  ): TemplateType =
    innerTemplatePattern.findFirstMatchIn(content) match {
      case Some(innerTemplate) =>
        val start = content.substring(0, innerTemplate.start)
        val end   = content.substring(innerTemplate.end)
        TemplateWithInnerLoop(endpoint, start, end, innerTemplate.group(1), headers, nullPayloadHandler)
      case None =>
        SimpleTemplate(endpoint, content, headers, nullPayloadHandler)
    }
}

trait TemplateType {
  def endpoint: String
  def headers:  Headers

  def renderRecords(record: NonEmptySeq[SinkRecord]): Either[SubstitutionError, NonEmptySeq[RenderedRecord]]

  def process(records: NonEmptySeq[RenderedRecord], tidyJson: Boolean): Either[SubstitutionError, ProcessedTemplate]
}

// this template type will require individual requests, the messages can't be batched
case class SimpleTemplate(
  endpoint:           String,
  content:            String,
  headers:            Headers,
  nullPayloadHandler: NullPayloadHandler,
) extends TemplateType
    with LazyLogging {

  override def renderRecords(records: NonEmptySeq[SinkRecord]): Either[SubstitutionError, NonEmptySeq[RenderedRecord]] =
    RecordRenderer.renderRecords(records, endpoint, content, headers, nullPayloadHandler)

  override def process(
    records:  NonEmptySeq[RenderedRecord],
    tidyJson: Boolean,
  ): Either[SubstitutionError, ProcessedTemplate] =
    records.head match {
      case RenderedRecord(_, _, recordRendered, headersRendered, endpointRendered) =>
        logger.debug(
          s"Processed template with tidyJson=$tidyJson",
        )
        ProcessedTemplate(endpointRendered, recordRendered, headersRendered).asRight
    }
}

case class TemplateWithInnerLoop(
  endpoint:           String,
  prefixContent:      String,
  suffixContent:      String,
  innerTemplate:      String,
  headers:            Headers,
  nullPayloadHandler: NullPayloadHandler,
) extends TemplateType
    with LazyLogging {

  override def renderRecords(records: NonEmptySeq[SinkRecord]): Either[SubstitutionError, NonEmptySeq[RenderedRecord]] =
    records.map {
      record =>
        RecordRenderer.renderRecord(
          record,
          endpoint,
          innerTemplate,
          headers,
          nullPayloadHandler,
        )
    }.sequence

  override def process(
    records:  NonEmptySeq[RenderedRecord],
    tidyJson: Boolean,
  ): Either[SubstitutionError, ProcessedTemplate] = {

    val replaceWith = records.toSeq.flatMap(_.recordRendered).mkString("")
    val fnContextFix: String => String = content => {
      if (tidyJson) cleanUp(content) else content
    }
    val contentOrError = fnContextFix(prefixContent + replaceWith + suffixContent)
    logger.debug(
      s"Processed template with prefixContent=$prefixContent, suffixContent=$suffixContent, tidyJson=$tidyJson",
    )
    ProcessedTemplate(
      endpoint = records.head.endpointRendered,
      content  = contentOrError,
      headers  = records.toSeq.flatMap(_.headersRendered).distinct,
    ).asRight
  }

}
