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
import io.lenses.streamreactor.connect.aws.s3.sink.ProcessorException
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface

import scala.collection.mutable

/**
  * Blocking processor for queues of operations.  Used to ensure consistency.
  * Will block any further writes by the current file until the remote has caught up.
  *
  * @param storageInterface the storage interface of the remote service.
  */
class BlockingQueueProcessor()(implicit storageInterface: StorageInterface) extends LazyLogging {

  private val operations = mutable.Queue[ProcessorOperation]()

  private def formatQueue(q: Seq[ProcessorOperation]): String = {
    if (q.isEmpty) " (EMPTY)" else " \n * " + q.map(_.toString).mkString("\n * ")
  }

  def enqueue(operationData: ProcessorOperation*): Unit = {
    logger.trace(s"Queueing ${operationData.size} operation(s): ${formatQueue(operationData)}")
    operations.enqueue(operationData: _*)
  }

  def hasOperations: Boolean = operations.nonEmpty

  def process(): Either[ProcessorException, Unit] = {
    logger.trace(s"Processing Queue: ${formatQueue(operations)}")
    while (operations.nonEmpty) {
      val operation = operations.head
      operation.process match {
        case Left(exception) =>
          logger.error("Exception in BlockingQueueProcessor", exception)
          return exception.asLeft
        case Right(_) =>
          operations.dequeue()
          logger.trace(s"Successfully processed $operation, updated queue: ${formatQueue(operations)}")
      }
    }

    ().asRight
  }

}
