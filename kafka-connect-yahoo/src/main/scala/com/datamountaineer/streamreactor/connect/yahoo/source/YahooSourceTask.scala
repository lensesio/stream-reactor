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

package com.datamountaineer.streamreactor.connect.yahoo.source

import java.util
import java.util.logging.Logger

import com.datamountaineer.streamreactor.connect.utils.ProgressCounter
import com.datamountaineer.streamreactor.connect.yahoo.config.{YahooSettings, YahooSourceConfig}
import org.apache.kafka.common.config.{AbstractConfig, ConfigException}
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}


class YahooSourceTask extends SourceTask with YahooSourceConfig {
  val logger: Logger = Logger.getLogger(getClass.getName)
  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false

  private var taskConfig: Option[AbstractConfig] = None
  private var dataManager: Option[DataRetrieverManager] = None

  /**
    * Starts the Yahoo source, parsing the options and setting up the reader.
    *
    * @param props A map of supplied properties.
    **/
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/yahoo-ascii.txt")).mkString)

    //get configuration for this task
    taskConfig = Try(new AbstractConfig(configDef, props)) match {
      case Failure(f) => throw new ConfigException("Couldn't start YahooSource due to configuration error.", f)
      case Success(s) => Some(s)
    }

    val settings = YahooSettings(taskConfig.get)

    dataManager = Some(DataRetrieverManager(
      YahooDataRetriever,
      settings.fxQuotes.toArray,
      settings.fxKafkaTopic,
      settings.stocks.toArray,
      settings.stocksKafkaTopic,
      settings.pollInterval,
      settings.bufferSize))

    dataManager.foreach(_.start())
    logger.info("Data manager started")

  }

  /**
    * Called by the Framework
    *
    * Checks the Yahoo data retriever for records of SourceRecords.
    *
    * @return A util.List of SourceRecords.
    **/
  override def poll(): util.List[SourceRecord] = {
    logger.info("Polling for Yahoo records...")
    val records = dataManager.map(_.getRecords).getOrElse(new util.ArrayList[SourceRecord]())
    if (enableProgress) {
      progressCounter.update(records.toVector)
    }
    records
  }

  /**
    * Stop the task and close readers.
    *
    **/
  override def stop(): Unit = {
    logger.info("Stopping Yahoo source...")
    dataManager.foreach(_.close())
    logger.info("Yahoo data retriever stopped.")
    progressCounter.empty
  }

  /**
    * Gets the version of this sink.
    *
    * @return
    */
  override def version(): String = getClass.getPackage.getImplementationVersion

}
