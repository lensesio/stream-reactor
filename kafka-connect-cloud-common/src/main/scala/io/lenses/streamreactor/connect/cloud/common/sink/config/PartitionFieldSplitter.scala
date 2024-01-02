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
package io.lenses.streamreactor.connect.cloud.common.sink.config

/**
  * A utility for splitting a string into parts while respecting backticks.
  */
object PartitionFieldSplitter {

  /**
    * Splits an input string into parts while respecting backticks.
    * Backticks are treated as complete strings and not subject to splitting.
    *
    * @param input The input string to split.
    * @return A sequence of strings representing the split parts.
    */
  def split(input: String): Seq[String] =
    input.foldLeft((Seq.empty[String], "", false)) {
      case ((result, currentPart, insideBacktick), char) =>
        (char, currentPart, insideBacktick) match {
          case ('`', "", false)   => (result, currentPart, true)
          case ('`', part, true)  => (result :+ part, "", false)
          case ('.', "", false)   => (result, currentPart, false)
          case ('.', part, false) => (result :+ part, "", false)
          case (c, part, _)       => (result, part + c, insideBacktick)
        }
    } match {
      case (result, currentPart, _) =>
        if (currentPart.nonEmpty) result :+ currentPart
        else result
    }
}
