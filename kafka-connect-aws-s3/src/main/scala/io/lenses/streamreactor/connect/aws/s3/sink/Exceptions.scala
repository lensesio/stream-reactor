/*
 * Copyright 2021 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.sink

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.TopicPartition

trait SinkError {
  def message(): String
  def rollBack() : Boolean
  def topicPartitions() : Set[TopicPartition]
}

// Cannot be retried, must be cleaned up
case class FatalS3SinkError(message: String, exception: Throwable, topicPartition: TopicPartition) extends SinkError with LazyLogging {

  logger.error(message, exception)

  override def rollBack(): Boolean = true

  override def topicPartitions(): Set[TopicPartition] = Set(topicPartition)
}

case object FatalS3SinkError {

  def apply(message: String, topicPartition: TopicPartition): FatalS3SinkError = {
    FatalS3SinkError(message, new IllegalStateException(message), topicPartition)
  }

}

// Can be retried
case class NonFatalS3SinkError(message: String, exception: Throwable) extends SinkError with LazyLogging {

  logger.error(message, exception)

  override def rollBack(): Boolean = false

  override def topicPartitions(): Set[TopicPartition] = Set()
}

case object NonFatalS3SinkError {
  def apply(message: String): NonFatalS3SinkError = {
    NonFatalS3SinkError(message, new IllegalStateException(message))
  }

  def apply(exception: Throwable): NonFatalS3SinkError = {
    NonFatalS3SinkError(exception.getMessage, exception)
  }
}

case object BatchS3SinkError {
  def apply(mixedExceptions: Set[SinkError]): BatchS3SinkError = {
    BatchS3SinkError(
      mixedExceptions.collect {
        case fatal: FatalS3SinkError => fatal
      },
      mixedExceptions.collect {
        case fatal: NonFatalS3SinkError => fatal
      }
    )
  }
}

case class BatchS3SinkError(
                             fatal: Set[FatalS3SinkError],
                             nonFatal: Set[NonFatalS3SinkError]
                               ) extends SinkError with LazyLogging {

  logger.error("Batchs3sinkError fatal {}, nonFatal {}", fatal, nonFatal)


  def hasFatal: Boolean = fatal.nonEmpty


  override def message(): String = {
      "fatal:\n" + fatal.map(_.message).mkString("\n") + "\n\nnonFatal:\n" + nonFatal.map(_.message).mkString("\n") + "\n\nFatal TPs:\n" + fatal.map(_.topicPartitions())
  }

  override def rollBack(): Boolean = fatal.nonEmpty

  override def topicPartitions(): Set[TopicPartition] = fatal.map(_.topicPartition)
}
