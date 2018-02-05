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

package com.datamountaineer.streamreactor.connect.redis.sink

import java.util

import com.datamountaineer.streamreactor.connect.errors.ErrorPolicyEnum
import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisConfig, RedisConfigConstants, RedisSinkSettings}
import com.datamountaineer.streamreactor.connect.redis.sink.writer.{RedisCache, RedisInsertSortedSet, RedisMultipleSortedSets, RedisWriter}
import com.datamountaineer.streamreactor.connect.utils.{ProgressCounter, JarManifest}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConversions._

/**
  * <h1>RedisSinkTask</h1>
  *
  * Kafka Connect Redis sink task. Called by framework to put records to the
  * target sink
  **/
class RedisSinkTask extends SinkTask with StrictLogging {
  var writer: List[RedisWriter] = List[RedisWriter]()
  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  /**
    * Parse the configurations and setup the writer
    **/
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/redis-ascii.txt")).mkString + s" v $version")
    logger.info(manifest.printManifest())

    RedisConfig.config.parse(props)
    val sinkConfig = new RedisConfig(props)
    val settings = RedisSinkSettings(sinkConfig)
    enableProgress = sinkConfig.getBoolean(RedisConfigConstants.PROGRESS_COUNTER_ENABLED)

    //if error policy is retry set retry interval
    if (settings.errorPolicy.equals(ErrorPolicyEnum.RETRY)) {
      context.timeout(sinkConfig.getInt(RedisConfigConstants.ERROR_RETRY_INTERVAL).toLong)
    }

    //-- Find out the Connector modes (cache | INSERT (SortedSet) | PK (SortedSetS)

    // Cache mode requires >= 1 PK and *NO* STOREAS SortedSet setting
    val modeCache = settings.copy(kcqlSettings = settings.kcqlSettings.filter(_.kcqlConfig.getStoredAs == null).filter(_.kcqlConfig.getPrimaryKeys.size() >= 1))
    // Insert Sorted Set mode requires: target name of SortedSet to be defined and STOREAS SortedSet syntax to be provided
    val mode_INSERT_SS = settings.copy(kcqlSettings = settings.kcqlSettings.filter(_.kcqlConfig.getStoredAs == "SortedSet").filter(_.kcqlConfig.getTarget.length > 0))
    // Multiple Sorted Sets mode requires: 1 Primary Key to be defined and STORE SortedSet syntax to be provided
    val mode_PK_SS = settings.copy(kcqlSettings = settings.kcqlSettings.filter(_.kcqlConfig.getStoredAs == "SortedSet").filter(_.kcqlConfig.getPrimaryKeys.length == 1))

    //-- Start as many writers as required
    writer =
      (modeCache.kcqlSettings.headOption.map { _ =>
        logger.info("Starting " + modeCache.kcqlSettings.size + " KCQLs with Redis Cache mode")
        List(new RedisCache(modeCache))
      } ++ mode_INSERT_SS.kcqlSettings.headOption.map { _ =>
        logger.info("Starting " + mode_INSERT_SS.kcqlSettings.size + " KCQLs with Redis Insert Sorted Set mode")
        List(new RedisInsertSortedSet(mode_INSERT_SS))
      } ++ mode_PK_SS.kcqlSettings.headOption.map { _ =>
        logger.info("Starting " + mode_PK_SS.kcqlSettings.size + " KCQLs with Redis Multiple Sorted Sets mode")
        List(new RedisMultipleSortedSets(modeCache))
      }).flatten.toList

    require(writer.nonEmpty, s"No writers set for ${RedisConfigConstants.KCQL_CONFIG}!")
  }

  /**
    * Pass the SinkRecords to the writer for Writing
    **/
  override def put(records: util.Collection[SinkRecord]): Unit = {
    if (records.isEmpty) {
      logger.info("Empty list of records received.")
    }
    else {
      require(writer.nonEmpty, "Writer is not set!")
      val seq = records.toVector
      writer.foreach(w => w.write(seq))

      if (enableProgress) {
        progressCounter.update(seq)
      }
    }
  }

  /**
    * Clean up Cassandra connections
    **/
  override def stop(): Unit = {
    logger.info("Stopping Redis sink.")
    writer.foreach(w => w.close())
    progressCounter.empty
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    //TODO
    //have the writer expose a is busy; can expose an await using a countdownlatch internally
  }

  override def version: String = manifest.version()
}
