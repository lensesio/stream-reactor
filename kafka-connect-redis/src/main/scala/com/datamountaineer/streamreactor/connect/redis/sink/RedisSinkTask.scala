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

package com.datamountaineer.streamreactor.connect.redis.sink

import java.util

import com.datamountaineer.streamreactor.connect.errors.ErrorPolicyEnum
import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisSinkConfig, RedisSinkSettings}
import com.datamountaineer.streamreactor.connect.redis.sink.writer.{RedisCache, RedisInsertSortedSet, RedisMultipleSortedSets, RedisWriter}
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
  var writer = List[RedisWriter]()

  /**
    * Parse the configurations and setup the writer
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
        |       ____           ___      _____ _       __
        |      / __ \___  ____/ (_)____/ ___/(_)___  / /__
        |     / /_/ / _ \/ __  / / ___/\__ \/ / __ \/ //_/
        |    / _, _/  __/ /_/ / (__  )___/ / / / / / ,<
        |   /_/ |_|\___/\__,_/_/____//____/_/_/ /_/_/|_|
        |
        |  By Stefan Bocutiu & Antonios Chalkiopoulos
      """.stripMargin)

    RedisSinkConfig.config.parse(props)
    val sinkConfig = new RedisSinkConfig(props)
    val settings = RedisSinkSettings(sinkConfig)

    //if error policy is retry set retry interval
    if (settings.errorPolicy.equals(ErrorPolicyEnum.RETRY)) {
      context.timeout(sinkConfig.getInt(RedisSinkConfig.ERROR_RETRY_INTERVAL).toLong)
    }

    //-- Find out the Connector modes (cache | INSERT (SortedSet) | PK (SortedSetS)

    // Cache mode requires >= 1 PK and *NO* STOREAS SortedSet setting
    val modeCache = settings.copy(kcqlSettings = settings.kcqlSettings.filter(_.kcqlConfig.getStoredAs == null).filter(_.kcqlConfig.getPrimaryKeys.hasNext))
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
      writer.foreach(w => w.write(records.toSeq))
    }
  }

  /**
    * Clean up Cassandra connections
    **/
  override def stop(): Unit = {
    logger.info("Stopping Redis sink.")
    writer.foreach(w => w.close())
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    //TODO
    //have the writer expose a is busy; can expose an await using a countdownlatch internally
  }

  override def version(): String = getClass.getPackage.getImplementationVersion
}
