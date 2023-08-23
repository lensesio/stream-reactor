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

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.aws.s3.formats.writer.ArraySinkData
import io.lenses.streamreactor.connect.aws.s3.formats.writer.MapSinkData
import io.lenses.streamreactor.connect.aws.s3.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.aws.s3.formats.writer.StringSinkData

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.MutableMapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

/**
  * Ensures the string values new lines are escaped. The transformer is used for JSON storage.
  * It is expected the StructSinkData was already transformed to a MapSinkData
  */
class EscapeStringNewLineTransformer extends Transformer {
  override def transform(message: MessageDetail): Either[RuntimeException, MessageDetail] = {
    val value = message.value
    val newValue = value match {
      case StringSinkData(str, schema)  => StringSinkData(str.replaceAll("\n", "\\\\n"), schema)
      case MapSinkData(value, schema)   => MapSinkData(convert(value).asInstanceOf[java.util.Map[_, _]], schema)
      case ArraySinkData(value, schema) => ArraySinkData(convert(value).asInstanceOf[java.util.List[_]], schema)
      case _                            => value
    }
    message.copy(value = newValue).asRight
  }

  private def convert(any: Any): Any =
    any match {
      case s: String              => s.replaceAll("\n", "\\\\n")
      case m: java.util.Map[_, _] => m.asScala.map { case (k, v) => (convert(k), convert(v)) }.asJava
      case l: java.util.List[_]   => l.asScala.map(convert).asJava
      case a: Array[_]            => a.map(convert)
      case other => other
    }
}
