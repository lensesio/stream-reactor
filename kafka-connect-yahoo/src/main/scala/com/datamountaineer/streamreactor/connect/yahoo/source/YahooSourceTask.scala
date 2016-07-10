/**
  * Copyright 2016 Datamountaineer.
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
  **/

package com.datamountaineer.streamreactor.connect.yahoo.source

import java.util

import com.datamountaineer.streamreactor.connect.yahoo.config.{YahooSettings, YahooSourceConfig}
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.confluent.common.config.{AbstractConfig, ConfigException}
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.util.{Failure, Success, Try}


class YahooSourceTask extends SourceTask with StrictLogging with YahooSourceConfig {
  private var taskConfig: Option[AbstractConfig] = None
  private var yahooDataRetriever: Option[YahooDataRetriever] = None

  /**
    * Starts the Yahoo source, parsing the options and setting up the reader.
    *
    * @param props A map of supplied properties.
    **/
  override def start(props: util.Map[String, String]): Unit = {

    //get configuration for this task
    taskConfig = Try(new AbstractConfig(configDef, props)) match {
      case Failure(f) => throw new ConfigException("Couldn't start YahooSource due to configuration error.", f)
      case Success(s) => Some(s)
    }

    val settings = YahooSettings(taskConfig.get)

    yahooDataRetriever = Some(YahooDataRetriever(settings.fxQuotes.toArray,
      settings.fxKafkaTopic,
      settings.stocks.toArray,
      settings.stocksKafkaTopic,
      settings.pollInterval))

    yahooDataRetriever.foreach(_.start())
  }

  /**
    * Called by the Framework
    *
    * Checks the Yahoo data retriever for records of SourceRecords.
    *
    * @return A util.List of SourceRecords.
    **/
  override def poll(): util.List[SourceRecord] = yahooDataRetriever.map(_.getRecords()).orNull

  /**
    * Stop the task and close readers.
    *
    **/
  override def stop(): Unit = {
    logger.info("Stopping Yahoo source...")
    yahooDataRetriever.foreach(_.close())
    logger.info("Yahoo data retriever stopped.")
  }

  /**
    * Gets the version of this sink.
    *
    * @return
    */
  override def version(): String = getClass.getPackage.getImplementationVersion

}
