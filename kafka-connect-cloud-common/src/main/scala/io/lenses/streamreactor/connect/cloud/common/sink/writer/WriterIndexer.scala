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
package io.lenses.streamreactor.connect.cloud.common.sink.writer

import cats.implicits.catsSyntaxOptionId
import cats.implicits.toTraverseOps
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata

/**
  * The `WriterIndexer` class provides the indexing operations for a Writer.
  * It handles the optional nature of the `IndexManager` and provides methods for writing and cleaning the index.
  *
  * @constructor create a new `WriterIndexer` with an optional `IndexManager`.
  * @param maybeIndexManager An optional `IndexManager`. If provided, `WriterIndexer` will use it to perform index operations.
  * @tparam SM The type of `FileMetadata` used by the `IndexManager`.
  */
class WriterIndexer[SM <: FileMetadata](maybeIndexManager: Option[IndexManager[SM]]) {

  /**
    * Executes a function with the `IndexManager` and a provided value if both are present.
    *
    * @param option An optional value of type `A`. If present, the function `f` will be executed with this value and the `IndexManager`.
    * @param f      A function that takes an `IndexManager` and a value of type `A` and returns an `Either[SinkError, B]`.
    * @tparam A The type of the optional value.
    * @tparam B The type of the result of the function `f`.
    * @return An `Either[SinkError, Option[B]]` that contains a `SinkError` if the operation failed, or an `Option[B]` with the result of the function `f` if the operation succeeded.
    */
  private def withIndexManager[A, B](
    option: Option[A],
  )(f:      (IndexManager[SM], A) => Either[SinkError, B],
  ): Either[SinkError, Option[B]] = {
    for {
      indexManager <- maybeIndexManager
      value        <- option
    } yield f(indexManager, value)
  }.sequence

  /**
    * Writes an index entry.
    *
    * @param topicPartition The `TopicPartition` for which to write the index entry.
    * @param bucket The bucket in which the index entry is to be written.
    * @param uncommittedOffset The `Offset` to be written to the index.
    * @param path The path where the index entry is to be written.
    * @return An `Either` that contains a `SinkError` if the operation failed, or an `Option[String]` with the index entry if the operation succeeded.
    */
  def writeIndex(
    topicPartition:    TopicPartition,
    bucket:            String,
    uncommittedOffset: Offset,
    path:              String,
  ): Either[SinkError, Option[String]] =
    withIndexManager(path.some) { (indexManager, path) =>
      indexManager.write(bucket, path, topicPartition.withOffset(uncommittedOffset))
    }

  /**
    * Cleans an index entry.
    *
    * @param topicPartition The `TopicPartition` for which to clean the index entry.
    * @param key The `CloudLocation` of the index entry to be cleaned.
    * @param maybeIndexFileName An optional index file name. If provided, the method will clean the index entry in this file.
    * @return An `Either` that contains a `SinkError` if the operation failed, or an `Option[Int]` with the result of the clean operation if the operation succeeded.
    */
  def cleanIndex(
    topicPartition:     TopicPartition,
    key:                CloudLocation,
    maybeIndexFileName: Option[String],
  ): Either[SinkError, Option[Int]] =
    withIndexManager(maybeIndexFileName) { (indexManager, indexFileName) =>
      indexManager.clean(key.bucket, indexFileName, topicPartition)
    }

}
