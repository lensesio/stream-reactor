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
package com.landoop.streamreactor.connect.hive.sink.staging

import com.landoop.streamreactor.connect.hive.Offset
import com.landoop.streamreactor.connect.hive.Topic

import scala.util.Try

trait FilenamePolicy {
  val prefix: String
}

object DefaultFilenamePolicy extends FilenamePolicy {
  val prefix = "streamreactor"
}

object CommittedFileName {

  private val Regex = s"(.+)_(.+)_(\\d+)_(\\d+)_(\\d+)".r

  def unapply(filename: String): Option[(String, Topic, Int, Offset, Offset)] =
    filename match {
      case Regex(prefix, topic, partition, start, end) =>
        Try((prefix, Topic(topic), partition.toInt, Offset(start.toLong), Offset(end.toLong))).toOption
      case _ => None
    }
}
