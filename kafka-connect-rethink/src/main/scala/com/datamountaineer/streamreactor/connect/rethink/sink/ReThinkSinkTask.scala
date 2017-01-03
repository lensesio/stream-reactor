/*
 * *
 *   * Copyright 2016 Datamountaineer.
 *   *
 *   * Licensed under the Apache License, Version 2.0 (the "License");
 *   * you may not use this file except in compliance with the License.
 *   * You may obtain a copy of the License at
 *   *
 *   * http://www.apache.org/licenses/LICENSE-2.0
 *   *
 *   * Unless required by applicable law or agreed to in writing, software
 *   * distributed under the License is distributed on an "AS IS" BASIS,
 *   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   * See the License for the specific language governing permissions and
 *   * limitations under the License.
 *   *
 */

package com.datamountaineer.streamreactor.connect.rethink.sink

import java.util
import java.util.{Timer, TimerTask}

import com.datamountaineer.streamreactor.connect.rethink.config.ReThinkSinkConfig
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by andrew@datamountaineer.com on 24/03/16. 
  * stream-reactor
  */
class ReThinkSinkTask extends SinkTask with StrictLogging {
  private var writer : Option[ReThinkWriter] = None
  private val counter = mutable.Map.empty[String, Long]
  private val timer = new Timer()

  class LoggerTask extends TimerTask {
    override def run(): Unit = logCounts()
  }

  def logCounts(): mutable.Map[String, Long] = {
    counter.foreach( { case (k,v) => logger.info(s"Delivered $v records for $k.") })
    counter.empty
  }

  /**
    * Parse the configurations and setup the writer
    * */
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/rethink-sink-ascii.txt")).mkString)
    val sinkConfig = ReThinkSinkConfig(props)
    writer = Some(ReThinkWriter(config = sinkConfig, context = context))
    timer.schedule(new LoggerTask, 0, 60000)
  }

  /**
    * Pass the SinkRecords to the writer for Writing
    * */
  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    writer.foreach(w => w.write(records.toList))
    records.foreach(r => counter.put(r.topic() , counter.getOrElse(r.topic(), 0L) + 1L))
  }

  /**
    * Clean up writer
    * */
  override def stop(): Unit = {
    logger.info("Stopping Rethink sink.")
    writer.foreach(w => w.close())
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {}
  override def version(): String = getClass.getPackage.getImplementationVersion

}
