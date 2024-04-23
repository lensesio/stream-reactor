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
package io.lenses.streamreactor.connect.cloud.common.sink

import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition

trait SinkError {
  def exception(): Option[Throwable]

  def message(): String

  def rollBack(): Boolean

  def topicPartitions(): Set[TopicPartition]
}

// Cannot be retried, must be cleaned up
case class FatalCloudSinkError(message: String, exception: Option[Throwable], topicPartition: TopicPartition)
    extends SinkError {

  override def rollBack(): Boolean = true

  override def topicPartitions(): Set[TopicPartition] = Set(topicPartition)
}

case object FatalCloudSinkError {

  def apply(message: String, topicPartition: TopicPartition): FatalCloudSinkError =
    FatalCloudSinkError(message, Option.empty, topicPartition)

}

// Can be retried
case class NonFatalCloudSinkError(message: String, exception: Option[Throwable]) extends SinkError {

  override def rollBack(): Boolean = false

  override def topicPartitions(): Set[TopicPartition] = Set()
}

case object NonFatalCloudSinkError {
  def apply(message: String): NonFatalCloudSinkError =
    NonFatalCloudSinkError(message, Option.empty)

  def apply(exception: Throwable): NonFatalCloudSinkError =
    NonFatalCloudSinkError(exception.getMessage, exception.some)
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

  override def exception(): Option[Throwable] =
    fatal.++(nonFatal)
      .headOption
      .flatMap { ex: SinkError => ex.exception() }

  override def message(): String =
    "fatal:\n" + fatal.map(_.message).mkString("\n") + "\n\nnonFatal:\n" + nonFatal.map(_.message).mkString(
      "\n",
    ) + "\n\nFatal TPs:\n" + fatal.map(_.topicPartitions())

  override def rollBack(): Boolean = fatal.nonEmpty

  override def topicPartitions(): Set[TopicPartition] = fatal.map(_.topicPartition)
}
