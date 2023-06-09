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
  * Reads records from an input stream. A record is identified as the data contained between the prefix and suffix.
  * The prefix and suffix are included in the line.
  * @param input
  * @param prefix
  * @param suffix
  */
class PrefixSuffixReader(input: InputStream, prefix: String, suffix: String,  trim: Boolean) extends LineReader {
  private val br         = new BufferedReader(new InputStreamReader(input))
  private val suffixSize = suffix.length
  private var currentLine: String = ""

  //Returns the next record or None if there are no more
  def next(): Option[String] = {
    //read until we find the prefix. If we don't find it, return None
    readUntilPrefixOrNone()
      .flatMap {
        case PrefixReadResult(line, index) =>
          currentLine = line.substring(index + prefix.length)
          //check if the suffix is on the same line as the prefix. Grab the content between suffix and prefix as the value
          // and keep the remaining content for the next time next() is called. Else read until the suffix is found
          val suffixIndex = line.indexOf(suffix, index)
          if (suffixIndex > 0) {
            currentLine = line.substring(suffixIndex + suffixSize)
            Some(line.substring(index, suffixIndex + suffixSize))
          } else {
            // No suffix so "clear" the current line
            currentLine = ""
            val value = line.substring(index)
            readUntilSuffixOrNone(value).flatMap {
              case SuffixReadResult(line, index, builder) =>
                currentLine = line.substring(index + suffixSize)
                if (trim) {
                  builder.append(line.substring(0, index + suffixSize).trim)
                } else {
                  builder.append(line.substring(0, index + suffixSize))
                }
                Some(builder.toString())
            }
          }
      }
  }

  def close(): Unit =
    input.close()

  private def readUntilPrefixOrNone(): Option[PrefixReadResult] = {
    //if the currentLine contains the suffix then return it
    val currentLineIndex = currentLine.indexOf(prefix)
    if (currentLineIndex >= 0) {
      Some(PrefixReadResult(currentLine, currentLineIndex))
    } else {
      var line  = br.readLine()
      var index = Option(line).map(_.indexOf(prefix)).getOrElse(-1)
      while (line != null && index < 0) {
        line  = br.readLine()
        index = Option(line).map(_.indexOf(prefix)).getOrElse(-1)
      }
      Option(line).map(l => PrefixReadResult(l, index))
    }
  }

  private def readUntilSuffixOrNone(initial: String): Option[SuffixReadResult] = {
    var line  = br.readLine()
    val sb    = new StringBuilder(initial)
    var index = Option(line).map(_.indexOf(suffix)).getOrElse(-1)
    while (line != null && index < 0) {

      if (trim) {
        sb.append(line.trim)
      } else {
        sb.append(System.lineSeparator())
        sb.append(line)
      }
      line  = br.readLine()
      index = Option(line).map(_.indexOf(suffix)).getOrElse(-1)
    }
    Option(line).map { l =>
      if (!trim) {
        sb.append(System.lineSeparator())
      }
      SuffixReadResult(l, index, sb)
    }
  }

  private case class PrefixReadResult(line: String, index: Int)
  private case class SuffixReadResult(line: String, index: Int, builder: StringBuilder)

}
