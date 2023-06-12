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
import scala.annotation.tailrec

/**
  * Reads records from an input stream. A record is identified as the data contained between the prefix and suffix.
  * The prefix and suffix are included in the line.
  * @param input
  * @param prefix
  * @param suffix
  */
class PrefixSuffixReader(input: InputStream, prefix: String, suffix: String, bufferSize: Int = 1024)
    extends LineReader {
  private val buffer     = new Array[Char](bufferSize)
  private val br         = new BufferedReader(new InputStreamReader(input))
  private val suffixSize = suffix.length
  private var currentLine: String = new String(buffer)

  //Returns the next record or None if there are no more
  def next(): Option[String] =
    //read until we find the prefix. If we don't find it, return None
    readUntilPrefixOrNone(currentLine) match {
      case None =>
        currentLine = ""
        None
      case Some(line) =>
        currentLine = line
        val sb = new StringBuilder()
        readUntilSuffixOrNone(currentLine, sb) match {
          case None =>
            currentLine = ""
            None
          case Some(SuffixReadResult(line, builder)) =>
            currentLine = line
            Some(builder.toString())
        }
    }

  def close(): Unit =
    input.close()

  @tailrec
  private def readUntilPrefixOrNone(value: String): Option[String] = {
    //if the currentLine contains the suffix then return it
    val currentLineIndex = value.indexOf(prefix)
    if (currentLineIndex >= 0) {
      Some(value.substring(currentLineIndex))
    } else {
      TextUtils.partiallyEndsWith(value, prefix) match {
        case None =>
          val count = br.read(buffer)
          if (count == -1) None
          else {
            val line = new String(buffer, 0, count)
            readUntilPrefixOrNone(line)
          }
        case Some(value) =>
          val count = br.read(buffer)
          if (count == -1) None
          else {
            val line = new String(buffer, 0, count)
            readUntilPrefixOrNone(value + line)
          }
      }
    }
  }

  @tailrec
  private def readUntilSuffixOrNone(initial: String, sb: StringBuilder): Option[SuffixReadResult] = {
    val suffixIndex = initial.indexOf(suffix)
    if (suffixIndex >= 0) {
      val value = initial.substring(suffixIndex + suffixSize)
      sb.append(initial.substring(0, suffixIndex + suffixSize))
      Some(SuffixReadResult(value, sb))
    } else {
      TextUtils.partiallyEndsWith(initial, suffix) match {
        case None =>
          val count = br.read(buffer)
          if (count == -1) None
          else {
            val line = new String(buffer, 0, count)
            sb.append(initial)
            readUntilSuffixOrNone(line, sb)
          }
        case Some(value) =>
          val count = br.read(buffer)
          if (count == -1) None
          else {
            val line = new String(buffer, 0, count)
            sb.append(initial.substring(0, initial.length - value.length))
            readUntilSuffixOrNone(value + line, sb)
          }
      }
    }
  }

  private case class SuffixReadResult(line: String, builder: StringBuilder)

}
