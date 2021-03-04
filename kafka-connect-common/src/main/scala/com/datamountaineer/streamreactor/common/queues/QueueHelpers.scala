/*
 *
 *  * Copyright 2020 Lenses.io.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.datamountaineer.streamreactor.common.queues

import java.util
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import com.google.common.collect.Queues
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.source.SourceRecord

/**
  * Created by r on 3/1/16.
  */
object QueueHelpers extends StrictLogging {

  implicit class LinkedBlockingQueueExtension[T](val lbq: LinkedBlockingQueue[T]) extends AnyVal {
    def drainWithTimeoutTo(collection: util.Collection[_ >: T], maxElements: Int, timeout: Long, unit: TimeUnit): Int = {
      Queues.drain[T](lbq, collection, maxElements, timeout, unit)
    }
  }

  def drainWithTimeoutNoGauva(records: util.ArrayList[SourceRecord],
                              batchSize: Int,
                              lingerTimeout: Long,
                              queue: LinkedBlockingQueue[SourceRecord]): Unit = {
    var added = 0
    val deadline = System.nanoTime() + TimeUnit.NANOSECONDS.toNanos(lingerTimeout)

    //wait for batch size or linger, which ever is first
    while (added < batchSize) {
      added += queue.drainTo(records, batchSize - added)
      //still not at batch size, poll with timeout
      if (added < batchSize) {
        val record = queue.poll(deadline - System.nanoTime(), TimeUnit.NANOSECONDS)
        record match {
          case s: SourceRecord =>
            records.add(s)
            added += 1
          case _ => added = batchSize
        }
      }
    }
  }

  /**
    * Drain the queue with timeout
    *
    * @param queue     The queue to drain
    * @param batchSize Batch size to take
    * @param timeOut   Timeout to take the batch
    * @return ArrayList of T
    * */
  def drainQueueWithTimeOut[T](queue: LinkedBlockingQueue[T], batchSize: Int, timeOut: Long): util.ArrayList[T] = {
    val l = new util.ArrayList[T]()
    logger.debug(s"Found [{}]. Draining entries to batchSize [{}].", queue.size(), batchSize)
    queue.drainWithTimeoutTo(l, batchSize, timeOut, TimeUnit.MILLISECONDS)
    l
  }

  /**
    * Drain the queue
    *
    * @param queue     The queue to drain
    * @param batchSize Batch size to take
    * @return ArrayList of T
    * */
  def drainQueue[T](queue: LinkedBlockingQueue[T], batchSize: Int): util.ArrayList[T] = {
    val l = new util.ArrayList[T]()
    logger.debug(s"Found {}. Draining entries to batchSize [{}].", queue.size(), batchSize)
    queue.drainTo(l, batchSize)
    l
  }
}
