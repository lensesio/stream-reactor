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

package com.datamountaineer.streamreactor.connect.coap.sink

import java.util
import java.util.{Timer, TimerTask}

import com.datamountaineer.streamreactor.connect.coap.configs.{CoapConfig, CoapSettings}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.mutable
import scala.collection.JavaConversions._

/**
  * Created by andrew@datamountaineer.com on 29/12/2016. 
  * stream-reactor
  */
class CoapSinkTask extends SinkTask with StrictLogging {
  private val writers = mutable.Map.empty[String, CoapWriter]

  private val timer = new Timer()
  private val counter = mutable.Map.empty[String, Long]

  class LoggerTask extends TimerTask {
    override def run(): Unit = logCounts()
  }

  def logCounts(): mutable.Map[String, Long] = {
    counter.foreach( { case (k,v) => logger.info(s"Delivered $v records for $k.") })
    counter.empty
  }

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/coap-sink-ascii.txt")).mkString)
    val sinkConfig = CoapConfig(props)
    val settings = CoapSettings(sinkConfig, sink = true)
    settings.map(s => (s.kcql.getSource, CoapWriter(s))).map({ case (k,v) => writers.put(k,v)})
    timer.schedule(new LoggerTask, 0, 10000)
  }

  override def put(records: util.Collection[SinkRecord]): Unit = {
    records.map(r => writers(r.topic()).write(List(r)))
    records.foreach(r => counter.put(r.topic() , counter.getOrElse(r.topic(), 0L) + 1L))
  }

  override def stop(): Unit = {
    writers.foreach({ case (t, w) =>
      logger.info(s"Shutting down writer for $t")
      w.stop()
    })

  }
  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {}
  override def version(): String = "1"
}
