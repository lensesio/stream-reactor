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
import enumeratum.Enum
import io.lenses.streamreactor.connect.http.sink.tpl.substitutions.SubstitutionError
import io.lenses.streamreactor.connect.http.sink.tpl.substitutions.SubstitutionType
import org.apache.kafka.connect.sink.SinkRecord

import java.util.regex.Matcher
import scala.util.matching.Regex

class TemplateRenderer[X <: SubstitutionType](substitutionType: Enum[X]) {

  private val templatePattern: Regex = "\\{\\{([^{}]*)}}".r

  // Method to render a single data entry with a template
  def render(data: SinkRecord, tplText: String): Either[SubstitutionError, String] =
    Either.catchOnly[SubstitutionError](
      templatePattern
        .replaceAllIn(
          tplText,
          matchTag =>
            Matcher.quoteReplacement {
              val tag = Option(matchTag.group(1)).getOrElse("").trim
              getValue(tag, data)
                .leftMap(throw _)
                .merge
            },
        ),
    )

// Helper method to get the value for a given tag from data
  private[renderer] def getValue(tag: String, data: SinkRecord): Either[SubstitutionError, String] = {
    val tagOpt = Option(tag).filter(_.nonEmpty).toRight(SubstitutionError("No tag specified"))
    tagOpt.flatMap { t =>
      val locs    = t.split("\\.", 2)
      val key     = locs.headOption.map(_.toLowerCase).getOrElse("")
      val locator = locs.lift(1)

      (key, locator) match {
        case ("#message", _) | ("/message", _) => Right("")
        case (k, loc) =>
          for {
            sType <- substitutionType.withNameInsensitiveOption(k).toRight(
              SubstitutionError(s"Couldn't find `$k` SubstitutionType"),
            )
            value <- sType.get(loc, data).map(_.toString).leftMap(_ =>
              SubstitutionError("SubstitutionType returned null"),
            )
          } yield value
      }
    }
  }

}
