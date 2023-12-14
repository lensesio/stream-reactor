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
package io.lenses.streamreactor.connect.http.sink.tpl.binding

import com.github.mustachejava.reflect.BaseObjectHandler
import com.github.mustachejava.util.Wrapper
import com.github.mustachejava.Binding
import com.github.mustachejava.Code
import com.github.mustachejava.TemplateContext
import io.lenses.streamreactor.connect.http.sink.tpl.templates.SubstitutionType

import java.util
import scala.util.Try

class KafkaConnectObjectHandler extends BaseObjectHandler {

  override def createBinding(nameCouldBeNull: String, tc: TemplateContext, code: Code): Binding =
    splitOutSubstitution(nameCouldBeNull).map {
      case (substitutionType: SubstitutionType, locator: Option[String]) => substitutionType.toBinding(locator)
    }.orNull

  private def splitOutSubstitution(nameCouldBeNull: String): Option[(SubstitutionType, Option[String])] =
    Option(nameCouldBeNull)
      .flatMap {
        name =>
          val split = name.split("\\.", 2)
          for {
            subsType <- SubstitutionType.withNameInsensitiveOption(split.head)
          } yield {
            subsType -> Try(split(1)).toOption
          }
      }

  override def find(name: String, scopes: util.List[AnyRef]): Wrapper =
    null
}
