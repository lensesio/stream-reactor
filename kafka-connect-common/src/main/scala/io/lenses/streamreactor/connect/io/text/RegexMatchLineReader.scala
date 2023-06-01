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

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader

/**
  * Reads the lines from the input stream if the line starts with a given prefix
  *
  * @param input
  * @param prefix
  */
class RegexMatchLineReader(input: InputStream, regex: String, skip: Int) {
  if (skip < 0) throw new IllegalArgumentException("skip must be >= 0")
  private val br      = new BufferedReader(new InputStreamReader(input))
  private val pattern = regex.r.pattern
  private var skipped = skip <= 0

  //Returns the next line if the prefix matches regex. If there are no more lines, returns None
  def next(): Option[String] = {
    skipLines()
    var line = br.readLine()
    while (line != null && !pattern.matcher(line).matches()) {
      line = br.readLine()
    }
    Option(line)
  }

  private def skipLines(): Unit =
    if (!skipped) {
      LineSkipper.skipLines(br, skip)
      skipped = true
    }
}
