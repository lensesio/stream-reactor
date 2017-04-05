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

  private var taskConfig: Option[AbstractConfig] = None
  private var dataManager: Option[DataRetrieverManager] = None

  /**
    * Starts the Yahoo source, parsing the options and setting up the reader.
    *
    * @param props A map of supplied properties.
    **/
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(

      """
        |
        |    ____        __        __  ___                  __        _
        |   / __ \____ _/ /_____ _/  |/  /___  __  ______  / /_____ _(_)___  ___  ___  _____
        |  / / / / __ `/ __/ __ `/ /|_/ / __ \/ / / / __ \/ __/ __ `/ / __ \/ _ \/ _ \/ ___/
        | / /_/ / /_/ / /_/ /_/ / /  / / /_/ / /_/ / / / / /_/ /_/ / / / / /  __/  __/ /
        |/_____/\__,_/\__/\__,_/_/  /_/\____/\__,_/_/ /_/\__/\__,_/_/_/ /_/\___/\___/_/
        |         __  __      __               _____
        |         \ \/ /___ _/ /_  ____  ____ / ___/____  __  _______________
        |          \  / __ `/ __ \/ __ \/ __ \\__ \/ __ \/ / / / ___/ ___/ _ \
        |          / / /_/ / / / / /_/ / /_/ /__/ / /_/ / /_/ / /  / /__/  __/
        |         /_/\__,_/_/ /_/\____/\____/____/\____/\__,_/_/   \___/\___/
        |
        | By Stefan Bocutiu
        |
        | """.stripMargin)

    logger.info(
      s"""
         |Configuration for task
         |${props.asScala}
      """.stripMargin)
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
    //progressCounter.update(records.asScala.toSeq)
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
