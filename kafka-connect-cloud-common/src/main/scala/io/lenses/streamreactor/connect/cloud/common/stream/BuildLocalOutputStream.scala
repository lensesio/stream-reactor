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
package io.lenses.streamreactor.connect.cloud.common.stream

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError

import java.io.BufferedOutputStream
import scala.util.Try

class BuildLocalOutputStream(outputStream: BufferedOutputStream, topicPartition: TopicPartition)
    extends CloudOutputStream
    with LazyLogging {

  private var pointer = 0

  override def write(bytes: Array[Byte], startOffset: Int, numberOfBytes: Int): Unit = {

    require(bytes != null && bytes.nonEmpty, "Bytes must be provided")
    val endOffset = startOffset + numberOfBytes
    require(
      validateRange(startOffset, bytes.length) &&
        numberOfBytes > 0 &&
        validateRange(endOffset, bytes.length),
    )

    outputStream.write(bytes.slice(startOffset, endOffset))
    pointer += endOffset - startOffset
  }

  override def write(b: Int): Unit = {
    outputStream.write(b)
    pointer += 1
  }

  override def complete(): Either[SinkError, Unit] = {
    for {
      a <- Try(outputStream.close()).toEither
    } yield a
  }.leftMap {
    case se: SinkError => se
    case to: Throwable =>
      FatalCloudSinkError(to.getMessage, topicPartition)
  }

  private def validateRange(startOffset: Int, numberOfBytes: Int) = startOffset >= 0 && startOffset <= numberOfBytes

  override def getPointer: Long = pointer.toLong

}
