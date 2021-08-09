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

package io.lenses.streamreactor.connect.aws.s3.processing

import cats.implicits.catsSyntaxEitherId
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.{RemotePathLocation, TopicPartition}
import io.lenses.streamreactor.connect.aws.s3.sink.{MapKey, ProcessorException}
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface

import scala.collection.mutable

/**
  * Manages the lifecycle of BlockingQueueProcessors.
  *
  * @param storageInterface storage interface
  */
class ProcessorManager()(implicit storageInterface: StorageInterface) extends LazyLogging {


  private val processors = mutable.Map.empty[MapKey, BlockingQueueProcessor]

  /**
    * Returns a processor that can processor records for a particular topic and partition.
    * The writer will create a file inside the given directory if there is no open writer.
    */
  def processor(topicPartition: TopicPartition, tempBucketAndPath: RemotePathLocation): BlockingQueueProcessor = {
    val mapKey = MapKey(topicPartition, tempBucketAndPath)
    processors.getOrElseUpdate(mapKey, new BlockingQueueProcessor())
  }

  def catchUp(): Either[ProcessorException, Unit] = {

    logger.trace(" ====== CATCH UP START ======")

    val exceptions = processors
      .values
      .filter(_.hasOperations)
      .map(_.process())
      .collect { case Left(throwable) => throwable }

    logger.trace(" ======  CATCH UP END  ======")

    exceptions.size match {
      case 0 => ().asRight
      case _ => new ProcessorException(exceptions).asLeft
    }

  }

  def cleanUp(topicPartition: TopicPartition): Unit = {
    processors
      .filterKeys(mapKey => mapKey
        .topicPartition == topicPartition)
      .keys
      .foreach({
        processors.remove
      })
  }

}
