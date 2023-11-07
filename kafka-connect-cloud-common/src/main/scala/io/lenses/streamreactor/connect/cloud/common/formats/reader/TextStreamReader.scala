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
package io.lenses.streamreactor.connect.cloud.common.formats.reader

import io.lenses.streamreactor.connect.cloud.common.formats.FormatWriterException
import io.lenses.streamreactor.connect.cloud.common.source.config.ReadTextMode

import java.io.InputStream
import scala.io.Source
import scala.util.Try

object TextStreamReader {
  def apply(
    readTextMode: Option[ReadTextMode],
    input:        InputStream,
  ): CloudDataIterator[String] =
    readTextMode.map(_.createStreamReader(input))
      .getOrElse(
        new TextStreamReader(
          input,
        ),
      )
}

class TextStreamReader(input: InputStream) extends CloudDataIterator[String] {

  private val source = Source.fromInputStream(input, "UTF-8")
  protected val sourceLines: Iterator[String] = source.getLines()

  override def close(): Unit = {
    Try(source.close())
    ()
  }

  override def hasNext: Boolean = sourceLines.hasNext

  override def next(): String = {
    if (!sourceLines.hasNext) {
      throw FormatWriterException(
        "Invalid state reached: the file content has been consumed, no further calls to next() are possible.",
      )
    }
    sourceLines.next()
  }

}
