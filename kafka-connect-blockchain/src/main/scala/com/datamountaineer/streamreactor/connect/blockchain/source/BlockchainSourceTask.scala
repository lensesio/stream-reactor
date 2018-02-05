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

package com.datamountaineer.streamreactor.connect.blockchain.source

import java.util

import com.datamountaineer.streamreactor.connect.blockchain.config.{BlockchainConfig, BlockchainConfigConstants, BlockchainSettings}
import com.datamountaineer.streamreactor.connect.utils.{ProgressCounter, JarManifest}
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.confluent.common.config.ConfigException
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class BlockchainSourceTask extends SourceTask with StrictLogging {

  private var taskConfig: Option[AbstractConfig] = None
  private var blockchainManager: Option[BlockchainManager] = None
  private val progressCounter = ProgressCounter()
  private var enableProgress: Boolean = false
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  /**
    * Starts the Blockchain source, parsing the options and setting up the reader.
    *
    * @param props A map of supplied properties.
    **/
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/blockchain-ascii.txt")).mkString + s" v $version")
    logger.info(manifest.printManifest())

    //get configuration for this task
    taskConfig = Try(new AbstractConfig(BlockchainConfig.config, props)) match {
      case Failure(f) => throw new ConfigException("Couldn't start BlockchainSource due to configuration error.", f)
      case Success(s) => Some(s)
    }
    enableProgress = taskConfig.get.getBoolean(BlockchainConfigConstants.PROGRESS_COUNTER_ENABLED)

    val settings = BlockchainSettings(taskConfig.get)

    blockchainManager = Some(new BlockchainManager(settings))
    blockchainManager.foreach(_.start())
    logger.info("Data manager started")
  }

  /**
    * Called by the Framework
    *
    * Checks the Blockchain manager for records of SourceRecords.
    *
    * @return A util.List of SourceRecords.
    **/
  override def poll(): util.List[SourceRecord] = {
    Thread.sleep(1000)
    val records = blockchainManager.map(_.get()).getOrElse(new util.ArrayList[SourceRecord]())
    logger.debug(s"Returning ${records.size()} record(-s) from Blockchain source")
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
    logger.info("Stopping Blockchain source...")
    blockchainManager.foreach(_.close())
    logger.info("Blockchain data retriever stopped.")
  }

  /**
    * Gets the version of this sink.
    *
    * @return
    */
  override def version: String = manifest.version()

}
