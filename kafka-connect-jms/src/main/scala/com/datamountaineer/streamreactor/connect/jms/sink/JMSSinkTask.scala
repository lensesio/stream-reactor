/*
 *  Copyright 2017 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.jms.sink

import java.util

import com.datamountaineer.streamreactor.connect.errors.ErrorPolicyEnum
import com.datamountaineer.streamreactor.connect.jms.config.{JMSConfig, JMSSettings}
import com.datamountaineer.streamreactor.connect.jms.sink.writer.JMSWriter
import com.datamountaineer.streamreactor.connect.utils.ProgressCounter
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConversions._

/**
  * <h1>JMSSinkTask</h1>
  *
  * Kafka Connect JMS sink task. Called by framework to put records to the target sink
  **/
class JMSSinkTask extends SinkTask with StrictLogging {

  var writer: Option[JMSWriter] = None
  val progressCounter = new ProgressCounter

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

    JMSConfig.config.parse(props)
    val sinkConfig = new JMSConfig(props)
    val settings = JMSSettings(sinkConfig, true)

    //if error policy is retry set retry interval
    if (settings.errorPolicy.equals(ErrorPolicyEnum.RETRY)) {
      context.timeout(sinkConfig.getInt(JMSConfig.ERROR_RETRY_INTERVAL).toLong)
    }

    writer = Some(JMSWriter(settings))
  }

  /**
    * Pass the SinkRecords to the writer for Writing
    **/
  override def put(records: util.Collection[SinkRecord]): Unit = {
    writer.foreach(w => w.write(records.toSeq))
    //progressCounter.update(records.asScala.toSeq)
  }

  /**
    * Clean up connections
    **/
  override def stop(): Unit = {
    logger.info("Stopping JMS sink.")
    writer.foreach(w => w.close())
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    //TODO
    //have the writer expose a is busy; can expose an await using a countdownlatch internally
  }

  override def version(): String = getClass.getPackage.getImplementationVersion
}
