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
package io.lenses.streamreactor.connect.cloud.common.sink.extractors

/**
  * Object responsible for splitting a path string into segments.
  * The path string is expected to contain segments separated by dots ('.').
  * Segments can be quoted to include dots as part of the segment.
  *
  * @example
  *          {{{
  *          QuotedPathSplitter.splitPath("'one.field.with.dots'.'second.field.with.dots'", "'")
  *          // returns Array("one.field.with.dots","second.field.with.dots")
  *          }}}
  */
object QuotedPathSplitter {

  /**
    * Splits a path string into segments.
    * The path string is expected to contain segments separated by dots ('.').
    * Segments can be quoted to include dots as part of the segment.
    *
    * @param path           The path string to split.
    * @param quoteCharacter The character used for quoting segments.
    * @return An array of path segments.
    * @example
    * {{{
    *          splitPath("'one.field.with.dots'.'second.field.with.dots'", "'")
    *          // returns Array("one.field.with.dots","second.field.with.dots")
    *           }}}
    */
  def splitPath(path: String, quoteCharacter: String): Array[String] = {
    val (lastPart, parts, _) = path.foldLeft(("", List.empty[String], false)) {
      case ((currentPart, parts, insideQuotes), char) =>
        if (char == '.' && !insideQuotes) ("", parts :+ currentPart, insideQuotes)
        else if (char.toString == quoteCharacter) (currentPart, parts, !insideQuotes)
        else (currentPart + char, parts, insideQuotes)
    }

    (parts :+ lastPart).map(part =>
      if (part.startsWith(quoteCharacter) && part.endsWith(quoteCharacter))
        part.substring(1, part.length - 1)
      else
        part,
    ).toArray
  }
}
