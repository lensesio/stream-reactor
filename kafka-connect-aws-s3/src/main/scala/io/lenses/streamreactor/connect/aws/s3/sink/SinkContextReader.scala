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
package io.lenses.streamreactor.connect.aws.s3.sink

import cats.implicits.catsSyntaxEitherId

import java.util
import java.util.Collections
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.Try

object SinkContextReader {

  def mergeProps(
    contextPropsFn: () => util.Map[String, String],
  )(fallbackProps:  util.Map[String, String],
  ): Either[String, util.Map[String, String]] = {
    val context  = Try(contextPropsFn()).toOption.getOrElse(Collections.emptyMap())
    val fallback = Option(fallbackProps).getOrElse(Collections.emptyMap())
    fallback.asScala.addAll(context.asScala).asJava.asRight
  }

}
