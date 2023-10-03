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
package io.lenses.streamreactor.connect.cloud.common.sink

import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition

trait SinkError {
  def exception(): Throwable

  def message(): String

  def rollBack(): Boolean

  def topicPartitions(): Set[TopicPartition]
}

// Cannot be retried, must be cleaned up
case class FatalCloudSinkError(message: String, exception: Throwable, topicPartition: TopicPartition)
    extends SinkError {

  override def rollBack(): Boolean = true

  override def topicPartitions(): Set[TopicPartition] = Set(topicPartition)
}

case object FatalCloudSinkError {

  def apply(message: String, topicPartition: TopicPartition): FatalCloudSinkError =
    FatalCloudSinkError(message, new IllegalStateException(message), topicPartition)

}

// Can be retried
case class NonFatalCloudSinkError(message: String, exception: Throwable) extends SinkError {

  override def rollBack(): Boolean = false

  override def topicPartitions(): Set[TopicPartition] = Set()
}

case object NonFatalCloudSinkError {
  def apply(message: String): NonFatalCloudSinkError =
    NonFatalCloudSinkError(message, new IllegalStateException(message))

  def apply(exception: Throwable): NonFatalCloudSinkError =
    NonFatalCloudSinkError(exception.getMessage, exception)
}

case object BatchCloudSinkError {
  def apply(mixedExceptions: Set[SinkError]): BatchCloudSinkError =
    BatchCloudSinkError(
      mixedExceptions.collect {
        case fatal: FatalCloudSinkError => fatal
      },
      mixedExceptions.collect {
        case fatal: NonFatalCloudSinkError => fatal
      },
    )
}

case class BatchCloudSinkError(
  fatal:    Set[FatalCloudSinkError],
  nonFatal: Set[NonFatalCloudSinkError],
) extends SinkError {

  def hasFatal: Boolean = fatal.nonEmpty

  override def exception(): Throwable =
    fatal.++(nonFatal)
      .headOption
      .map(_.exception)
      .getOrElse(new IllegalStateException("No exception found in BatchCloudSinkError"))

  override def message(): String =
    "fatal:\n" + fatal.map(_.message).mkString("\n") + "\n\nnonFatal:\n" + nonFatal.map(_.message).mkString(
      "\n",
    ) + "\n\nFatal TPs:\n" + fatal.map(_.topicPartitions())

  override def rollBack(): Boolean = fatal.nonEmpty

  override def topicPartitions(): Set[TopicPartition] = fatal.map(_.topicPartition)
}
