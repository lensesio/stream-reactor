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
package io.lenses.streamreactor.connect.io.text

import java.io.InputStream
import scala.io.BufferedSource
import scala.io.Source

/**
  * Reads the lines from the input stream if the line starts with a given prefix
  *
  * @param input
  * @param prefix
  */
class RegexMatchLineReader(input: InputStream, regex: String) extends LineReader {
  private val source: BufferedSource = Source.fromInputStream(input)
  private val iterator = source.getLines()
  private val pattern  = regex.r.pattern

  //Returns the next line if the prefix matches regex. If there are no more lines, returns None
  def next(): Option[String] =
    iterator.find(line => pattern.matcher(line).matches())

  override def close(): Unit = source.close()
}
