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

package com.datamountaineer.streamreactor.connect.cassandra.sink

import java.util

import com.datamountaineer.streamreactor.connect.cassandra.config.CassandraConfigSink
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * <h1>CassandraSinkTask</h1>
  *
  * Kafka Connect Cassandra sink task. Called by
  * framework to put records to the target sink
  **/
class CassandraSinkTask extends SinkTask with StrictLogging {
  private var writer: Option[CassandraJsonWriter] = None
  private var timestamp: Long = 0

  private val counter = mutable.Map.empty[String, Long]
  logger.info("Task initialising")

  private def logCounts(): mutable.Map[String, Long] = {
    counter.foreach({ case (k, v) => logger.info(s"Delivered $v records for $k.") })
    counter.empty
  }

  /**
    * Parse the configurations and setup the writer
    **/
  override def start(props: util.Map[String, String]): Unit = {
    val taskConfig = Try(new CassandraConfigSink(props)) match {
      case Failure(f) => throw new ConnectException("Couldn't start CassandraSink due to configuration error.", f)
      case Success(s) => s
    }

    logger.info(
      """
        |    ____        __        __  ___                  __        _
        |   / __ \____ _/ /_____ _/  |/  /___  __  ______  / /_____ _(_)___  ___  ___  _____
        |  / / / / __ `/ __/ __ `/ /|_/ / __ \/ / / / __ \/ __/ __ `/ / __ \/ _ \/ _ \/ ___/
        | / /_/ / /_/ / /_/ /_/ / /  / / /_/ / /_/ / / / / /_/ /_/ / / / / /  __/  __/ /
        |/_____/\__,_/\__/\__,_/_/  /_/\____/\__,_/_/ /_/\__/\__,_/_/_/ /_/\___/\___/_/
        |       ______                                __           _____ _       __
        |      / ____/___ _______________ _____  ____/ /________ _/ ___/(_)___  / /__
        |     / /   / __ `/ ___/ ___/ __ `/ __ \/ __  / ___/ __ `/\__ \/ / __ \/ //_/
        |    / /___/ /_/ (__  |__  ) /_/ / / / / /_/ / /  / /_/ /___/ / / / / / ,<
        |    \____/\__,_/____/____/\__,_/_/ /_/\__,_/_/   \__,_//____/_/_/ /_/_/|_|
        |
        | By Andrew Stevenson.""".stripMargin)
    writer = Some(CassandraWriter(connectorConfig = taskConfig, context = context))
  }

  /**
    * Pass the SinkRecords to the writer for Writing
    **/
  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    writer.foreach(w => w.write(records.toVector))
    records.foreach(r => counter.put(r.topic(), counter.getOrElse(r.topic(), 0L) + 1L))
    if (timestamp > 0) {
      val newTimestamp = System.currentTimeMillis()
      if (scala.concurrent.duration.SECONDS.toSeconds(newTimestamp - timestamp) >= 60) {
        logCounts()
      }
      timestamp = newTimestamp
    } else {
      timestamp = System.currentTimeMillis()
    }
  }

  /**
    * Clean up Cassandra connections
    **/
  override def stop(): Unit = {
    logger.info("Stopping Cassandra sink.")
    writer.foreach(w => w.close())
    counter.empty
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {}

  override def version(): String = "1"
}
