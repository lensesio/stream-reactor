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

/**
  * Read the input stream and uses a sequence number to identify the line. A line returned by the reader
  * can correspond to multiple lines in the underlying input. Consider this example input stream content
  *
  * {{{
  * header line
  * second header line
  * 1 first line
  * 2 second line
  * second line continued
  * 3 third line
  *
  * 4 fourth line
  * }}}
  * returns 4 lines
  * @param input
  */
class SequenceBasedLineReader(input: InputStream) extends LineReader {
  private val br           = new java.io.BufferedReader(new java.io.InputStreamReader(input))
  private var currentIndex = -1
  private var currentLine: Option[String] = None

  def next(): Option[String] =
    if (currentIndex == -1) {
      readUntilFirstIndexOrNone().map { line =>
        currentIndex = 1
        val result = readUntilNextIndexOrNone(line, currentIndex)
        currentIndex += 1
        currentLine = result.nextLine
        result.line
      }
    } else {
      currentLine match {
        case Some(line) =>
          val result = readUntilNextIndexOrNone(line, currentIndex)
          currentIndex += 1
          currentLine = result.nextLine
          Some(result.line)
        case None => None
      }
    }

  def close(): Unit = input.close()

  private def readUntilNextIndexOrNone(str: String, i: Int): ReadResult = {
    var line  = str
    val index = (i + 1).toString
    val sb    = new StringBuilder()
    sb.append(line)
    line = br.readLine()
    while (line != null && !line.startsWith(index)) {
      sb.append(System.lineSeparator())
      sb.append(line)
      line = br.readLine()
    }
    ReadResult(Option(line), sb.toString())
  }

  /**
    * Read to first sequence number and return the line.
    */
  private def readUntilFirstIndexOrNone(): Option[String] = {
    var line = br.readLine()
    while (line != null && !line.startsWith("1")) {
      line = br.readLine()
    }
    Option(line)
  }

  private case class ReadResult(nextLine: Option[String], line: String)
}
