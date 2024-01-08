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
import io.lenses.streamreactor.connect.http.sink.tpl.substitutions.SubstitutionError
import io.lenses.streamreactor.connect.http.sink.tpl.substitutions.SubstitutionType
import org.apache.kafka.connect.sink.SinkRecord

import scala.util.matching.Regex
object TemplateRenderer {

  private val templatePattern: Regex = "\\{\\{(.*?)}}".r

  // Method to render a single data entry with a template
  def render(data: SinkRecord, tplText: String): Either[SubstitutionError, String] =
    Either.catchOnly[SubstitutionError](
      templatePattern
        .replaceAllIn(
          tplText,
          matchTag => {
            val tag = matchTag.group(1).trim
            getValue(tag, data)
              .leftMap(throw _)
              .merge
          },
        ),
    )

  // Helper method to get the value for a given tag from data
  private def getValue(tag: String, data: SinkRecord): Either[SubstitutionError, String] = {
    val locs = tag.split("\\.", 2)
    (locs(0).toLowerCase, locs.lift(1)) match {
      case ("#message", _) => "".asRight
      case ("/message", _) => "".asRight
      case (key: String, locator: Option[String]) =>
        SubstitutionType.withNameInsensitiveOption(key) match {
          case Some(sType) => sType.get(locator, data).map(_.toString)
          case None        => SubstitutionError(s"Couldn't find $key SubstitutionType").asLeft
        }
    }
  }

}
