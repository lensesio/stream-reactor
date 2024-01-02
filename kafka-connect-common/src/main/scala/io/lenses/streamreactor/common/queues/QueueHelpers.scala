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
package io.lenses.streamreactor.common.queues

import com.typesafe.scalalogging.StrictLogging

import java.util
import java.util.concurrent.LinkedBlockingQueue

/**
  * Created by r on 3/1/16.
  */
object QueueHelpers extends StrictLogging {

  /**
    * Drain the queue
    *
    * @param queue     The queue to drain
    * @param batchSize Batch size to take
    * @return ArrayList of T
    */
  def drainQueue[T](queue: LinkedBlockingQueue[T], batchSize: Int): util.ArrayList[T] = {
    val l = new util.ArrayList[T]()
    logger.debug(s"Found {}. Draining entries to batchSize [{}].", queue.size(), batchSize)
    queue.drainTo(l, batchSize)
    l
  }
}
