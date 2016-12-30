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

package com.datamountaineer.streamreactor.connect.jms

import java.util
import java.util.{Timer, TimerTask}

import com.datamountaineer.streamreactor.connect.errors.ErrorPolicyEnum
import com.datamountaineer.streamreactor.connect.jms.sink.config.{JMSSettings, JMSSinkConfig}
import com.datamountaineer.streamreactor.connect.jms.sink.writer.JMSWriter
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * <h1>JMSSinkTask</h1>
  *
  * Kafka Connect JMS sink task. Called by framework to put records to the target sink
  **/
class JMSSinkTask extends SinkTask with StrictLogging {

  var writer: Option[JMSWriter] = None
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

    logger.info("""
                  |  _____        _        __  __                   _        _
                  | |  __ \      | |      |  \/  |                 | |      (_)
                  | | |  | | __ _| |_ __ _| \  / | ___  _   _ _ __ | |_ __ _ _ _ __   ___  ___ _ __
                  | | |  | |/ _` | __/ _` | |\/| |/ _ \| | | | '_ \| __/ _` | | '_ \ / _ \/ _ \ '__|
                  | | |__| | (_| | || (_| | |  | | (_) | |_| | | | | || (_| | | | | |  __/  __/ |
                  | |_____/ \__,_|\__\__,_|_|__|_|\___/ \__,_|_| |_|\__\__,_|_|_| |_|\___|\___|_|
                  |      | |              / ____(_)     | |   By Stefan Bocutiu
                  |      | |_ __ ___  ___| (___  _ _ __ | | __
                  |  _   | | '_ ` _ \/ __|\___ \| | '_ \| |/ /
                  | | |__| | | | | | \__ \____) | | | | |   <
                  |  \____/|_| |_| |_|___/_____/|_|_| |_|_|\_\
                  |.""".stripMargin)

    JMSSinkConfig.config.parse(props)
    val sinkConfig = new JMSSinkConfig(props)
    val settings = JMSSettings(sinkConfig)

    //if error policy is retry set retry interval
    if (settings.errorPolicy.equals(ErrorPolicyEnum.RETRY)) {
      context.timeout(sinkConfig.getInt(JMSSinkConfig.ERROR_RETRY_INTERVAL).toLong)
    }

    writer = Some(JMSWriter(settings))
    timer.schedule(new LoggerTask, 0, 10000)
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
      writer.foreach(w => w.write(records.toStream))
      records.foreach(r => counter.put(r.topic() , counter.getOrElse(r.topic(), 0L) + 1L))
    }
  }

  /**
    * Clean up Cassandra connections
    **/
  override def stop(): Unit = {
    logger.info("Stopping Hbase sink.")
    writer.foreach(w => w.close())
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    //TODO
    //have the writer expose a is busy; can expose an await using a countdownlatch internally
  }

  override def version(): String = getClass.getPackage.getImplementationVersion
}
