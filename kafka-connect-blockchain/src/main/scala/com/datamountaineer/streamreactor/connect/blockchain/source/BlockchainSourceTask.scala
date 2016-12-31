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

package com.datamountaineer.streamreactor.connect.blockchain.source

import java.util
import java.util.{Timer, TimerTask}

import com.datamountaineer.streamreactor.connect.blockchain.config.{BlockchainConfig, BlockchainSettings}
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.confluent.common.config.ConfigException
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class BlockchainSourceTask extends SourceTask with StrictLogging {

  private var taskConfig: Option[AbstractConfig] = None
  private var blockchainManager: Option[BlockchainManager] = None

  private val counter = mutable.Map.empty[String, Long]
  private val timer = new Timer()

  class LoggerTask extends TimerTask {
    override def run(): Unit = logCounts()
  }

  def logCounts(): mutable.Map[String, Long] = {
    counter.foreach({ case (k, v) => logger.info(s"Delivered $v records for $k.") })
    counter.empty
  }

  /**
    * Starts the Blockchain source, parsing the options and setting up the reader.
    *
    * @param props A map of supplied properties.
    **/
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/blockchain-ascii.txt")).mkString)
    logger.info("Blockchain Task configuration")
    props.foreach { case (k, v) => logger.info("   Key= " + k + "     Value=" + v) }
    //get configuration for this task
    taskConfig = Try(new AbstractConfig(BlockchainConfig.config, props)) match {
      case Failure(f) => throw new ConfigException("Couldn't start BlockchainSource due to configuration error.", f)
      case Success(s) => Some(s)
    }

    val settings = BlockchainSettings(taskConfig.get)

    blockchainManager = Some(new BlockchainManager(settings))
    blockchainManager.foreach(_.start())
    logger.info("Data manager started")
    timer.schedule(new LoggerTask, 0, 10000)
  }

  /**
    * Called by the Framework
    *
    * Checks the Blockchain manager for records of SourceRecords.
    *
    * @return A util.List of SourceRecords.
    **/
  override def poll(): util.List[SourceRecord] = {
    val records = blockchainManager.map(_.get()).getOrElse(new util.ArrayList[SourceRecord]())
    logger.debug(s"Returning ${records.size()} record(-s) from Blockchain source")
    records.foreach(r => counter.put(r.topic(), counter.getOrElse(r.topic(), 0L) + 1L))
    records
  }

  /**
    * Stop the task and close readers.
    *
    **/
  override def stop(): Unit = {
    logger.info("Stopping Blockchain source...")
    blockchainManager.foreach(_.close())
    logger.info("Blockchain data retriever stopped.")
  }

  /**
    * Gets the version of this sink.
    *
    * @return
    */
  override def version(): String = getClass.getPackage.getImplementationVersion

}
