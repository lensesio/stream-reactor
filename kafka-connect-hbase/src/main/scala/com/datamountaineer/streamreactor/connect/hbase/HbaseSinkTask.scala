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

package com.datamountaineer.streamreactor.connect.hbase

import java.util
import java.util.{Timer, TimerTask}

import com.datamountaineer.streamreactor.connect.errors.ErrorPolicyEnum
import com.datamountaineer.streamreactor.connect.hbase.config.{HbaseSettings, HbaseSinkConfig}
import com.datamountaineer.streamreactor.connect.hbase.writers.{HbaseWriter, WriterFactoryFn}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * <h1>HbaseSinkTask</h1>
  *
  * Kafka Connect Cassandra sink task. Called by framework to put records to the
  * target sink
  **/
class HbaseSinkTask extends SinkTask with StrictLogging {

  var writer: Option[HbaseWriter] = None
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
    **/
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(

      """
      |    ____        __        __  ___                  __        _
      |   / __ \____ _/ /_____ _/  |/  /___  __  ______  / /_____ _(_)___  ___  ___  _____
      |  / / / / __ `/ __/ __ `/ /|_/ / __ \/ / / / __ \/ __/ __ `/ / __ \/ _ \/ _ \/ ___/
      | / /_/ / /_/ / /_/ /_/ / /  / / /_/ / /_/ / / / / /_/ /_/ / / / / /  __/  __/ /
      |/_____/\\_,\\\\\\\__,_/_/  /_/\___\\\\\,\/_/ /_/\\_/\__,_/_/_/ /_/\___/\___/_/
      |      / / / / __ )____ _________ / ___/(_)___  / /__
      |     / /_/ / __  / __ `/ ___/ _ \\__ \/ / __ \/ //_/
      |    / __  / /_/ / /_/ (__  )  __/__/ / / / / / ,<
      |   /_/ /_/_____/\__,_/____/\___/____/_/_/ /_/_/|_|
      |
      |By Stefan Bocutiu""".stripMargin)

    HbaseSinkConfig.config.parse(props)
    val sinkConfig = HbaseSinkConfig(props)
    val hbaseSettings = HbaseSettings(sinkConfig)

    //if error policy is retry set retry interval
    if (hbaseSettings.errorPolicy.equals(ErrorPolicyEnum.RETRY)) {
      context.timeout(sinkConfig.getInt(HbaseSinkConfig.ERROR_RETRY_INTERVAL).toLong)
    }

    logger.info(
      s"""Settings:
          |$hbaseSettings
      """.stripMargin)
    writer = Some(WriterFactoryFn(hbaseSettings))
    timer.schedule(new LoggerTask, 0, 60000)
  }

  /**
    * Pass the SinkRecords to the writer for Writing
    **/
  override def put(records: util.Collection[SinkRecord]): Unit = {
    if (records.size() == 0) {
      logger.info("Empty list of records received.")
    }
    else {
      require(writer.nonEmpty, "Writer is not set!")
      writer.foreach(w => w.write(records.toSeq))
      records.foreach(r => counter.put(r.topic() , counter.getOrElse(r.topic(), 0L) + 1L))
    }
  }

  /**
    * Clean up Hbase connections
    **/
  override def stop(): Unit = {
    logger.info("Stopping Hbase sink.")
    writer.foreach(w => w.close())
  }

  override def version(): String = getClass.getPackage.getImplementationVersion

  override def flush(offsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {

  }
}
