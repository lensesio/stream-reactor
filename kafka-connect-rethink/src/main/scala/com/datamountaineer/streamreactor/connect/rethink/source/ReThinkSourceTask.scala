/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.rethink.source

import java.util

import com.datamountaineer.streamreactor.connect.queues.QueueHelpers
import com.datamountaineer.streamreactor.connect.rethink.config.{ReThinkConfigConstants, ReThinkSourceConfig}
import com.datamountaineer.streamreactor.connect.utils.ProgressCounter
import com.rethinkdb.RethinkDB
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.collection.JavaConversions._


/**
  * Created by andrew@datamountaineer.com on 22/09/16. 
  * stream-reactor
  */
class ReThinkSourceTask extends SourceTask with StrictLogging {
  private var readers: Set[ReThinkSourceReader] = _
  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/rethink-source-ascii.txt")).mkString)
    val config = ReThinkSourceConfig(props)
    enableProgress = config.getBoolean(ReThinkConfigConstants.PROGRESS_COUNTER_ENABLED)
    lazy val r = RethinkDB.r
    readers = ReThinkSourceReadersFactory(config, r)
    readers.foreach(_.start())
  }

  /**
    * Read from readers queue
    **/
  override def poll(): util.List[SourceRecord] = {
    val records = readers.flatMap(r => {
      val records = new util.ArrayList[SourceRecord]()
      //wait for batch size
      while (r.queue.size() > 0 && records.size() <= r.batchSize) {
        records.addAll(QueueHelpers.drainQueue(r.queue, r.batchSize))
      }
      records
    }).toList

    if (enableProgress && records.size > 0) {
      progressCounter.update(records.toVector)
    }
    records
  }

  /**
    * Shutdown connections
    **/
  override def stop(): Unit = {
    logger.info("Stopping ReThink source and closing connections.")
    readers.foreach(_.stop())
    progressCounter.empty
  }

  override def version(): String = getClass.getPackage.getImplementationVersion
}
