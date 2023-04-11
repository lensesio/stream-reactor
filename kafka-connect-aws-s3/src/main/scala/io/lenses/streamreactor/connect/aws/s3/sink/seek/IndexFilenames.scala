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
package io.lenses.streamreactor.connect.aws.s3.sink.seek

import io.lenses.streamreactor.connect.aws.s3.model.Offset

import scala.util.Try

object IndexFilenames {

  /**
    * Generate the filename for the index file.
    */
  def indexFilename(sinkName: String, topic: String, partition: Int, offset: Long): String =
    f"${indexForTopicPartition(sinkName, topic, partition)}$offset%020d"

  /**
    * Generate the directory of the index for a given topic and partition
    */
  def indexForTopicPartition(sinkName: String, topic: String, partition: Int): String =
    f".indexes/$sinkName/$topic/$partition%05d/"

  /**
    * Parses an index filename and returns an offset
    */
  def offsetFromIndex(indexFilename: String): Either[Throwable, Offset] = {
    val lastIndex = indexFilename.lastIndexOf("/")
    val (_, last) = indexFilename.splitAt(lastIndex + 1)

    Try(Offset(last.toLong)).toEither
  }

}
