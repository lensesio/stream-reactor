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
import com.datamountaineer.streamreactor.connect.redis.sink.writer._
import com.datamountaineer.streamreactor.connect.utils.{JarManifest, ProgressCounter}
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConverters._

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
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/redis-ascii.txt")).mkString + s" $version")
    logger.info(manifest.printManifest())

    val conf = if (context.configs().isEmpty) props else context.configs()

    RedisConfig.config.parse(conf)
    val sinkConfig = new RedisConfig(conf)
    val settings = RedisSinkSettings(sinkConfig)
    enableProgress = sinkConfig.getBoolean(RedisConfigConstants.PROGRESS_COUNTER_ENABLED)

    //if error policy is retry set retry interval
    if (settings.errorPolicy.equals(ErrorPolicyEnum.RETRY)) {
      context.timeout(sinkConfig.getInt(RedisConfigConstants.ERROR_RETRY_INTERVAL).toLong)
    }

    //-- Find out the Connector modes (cache | INSERT (SortedSet) | PK (SortedSetS)

    // Cache mode requires >= 1 PK and *NO* STOREAS SortedSet setting
    val modeCache = filterModeCache(settings)

    // Insert Sorted Set mode requires: target name of SortedSet to be defined and STOREAS SortedSet syntax to be provided
    val mode_INSERT_SS = filterModeInsertSS(settings)

    val mode_PUBSUB = filterModePubSub(settings)

    // Multiple Sorted Sets mode requires: 1 Primary Key to be defined and STORE SortedSet syntax to be provided
    val mode_PK_SS = filterModePKSS(settings)

    // Geo Add mode requires: >=1 PK and STOREAS GeoAdd syntax to be provided
    val mode_GEOADD = filterGeoAddMode(settings)

    val mode_STREAM = filterStream(settings)

    //-- Start as many writers as required
    writer = (modeCache.kcqlSettings.headOption.map { _ =>
      logger.info("Starting " + modeCache.kcqlSettings.size + " KCQLs with Redis Cache mode")
      val writer = new RedisCache(modeCache)
      writer.createClient(settings)
      List(writer)
    } ++ mode_INSERT_SS.kcqlSettings.headOption.map { _ =>
      logger.info("Starting " + mode_INSERT_SS.kcqlSettings.size + " KCQLs with Redis Insert Sorted Set mode")
      val writer = new RedisInsertSortedSet(mode_INSERT_SS)
      writer.createClient(settings)
      List(writer)
    } ++ mode_PUBSUB.kcqlSettings.headOption.map { _ =>
      logger.info("Starting " + mode_PUBSUB.kcqlSettings.size + " KCQLs with Redis PubSub mode")
      val writer = new RedisInsertSortedSet(mode_INSERT_SS)
      writer.createClient(settings)
      List(writer)
    } ++ mode_PK_SS.kcqlSettings.headOption.map { _ =>
      logger.info("Starting " + mode_PK_SS.kcqlSettings.size + " KCQLs with Redis Multiple Sorted Sets mode")
      val writer = new RedisMultipleSortedSets(mode_PK_SS)
      writer.createClient(settings)
      List(writer)
    } ++ mode_GEOADD.kcqlSettings.headOption.map { _ =>
      logger.info("Starting " + mode_STREAM.kcqlSettings.size + " KCQLs with Redis Stream mode")
      val writer = new RedisStreams(mode_STREAM)
      writer.createClient(settings)
      List(writer)
    }).flatten.toList

    require(writer.nonEmpty, s"No writers set for ${RedisConfigConstants.KCQL_CONFIG}!")
  }

  /**
    * Construct a RedisSinkSettings object containing all the kcqlConfigs that use the Cache mode.
    * This function will filter by the absence of the "STOREAS" keyword and the presence of primary keys.
    *
    * KCQL Example: INSERT INTO cache SELECT price FROM yahoo-fx PK symbol
    *
    * @param settings The RedisSinkSettings containing all kcqlConfigs.
    * @return A RedisSinkSettings object containing only the kcqlConfigs that use the Cache mode.
    */
  def filterModeCache(settings: RedisSinkSettings): RedisSinkSettings = settings.copy(kcqlSettings =
    settings.kcqlSettings
      .filter(k => k.kcqlConfig.getStoredAs == null
        && k.kcqlConfig.getPrimaryKeys.size() >= 1))

  /**
    * Construct a RedisSinkSettings object containing all the kcqlConfigs that use the Sorted Set mode.
    * This function will filter by the presence of the "STOREAS" keyword and a target, as well as the absence of primary keys.
    *
    * KCQL Example: INSERT INTO cpu_stats SELECT * FROM cpuTopic STOREAS SortedSet(score=timestamp)
    *
    * @param settings The RedisSinkSettings containing all kcqlConfigs.
    * @return A RedisSinkSettings object containing only the kcqlConfigs that use the Sorted Set mode.
    */
  def filterModeInsertSS(settings: RedisSinkSettings): RedisSinkSettings = settings.copy(kcqlSettings =
    settings.kcqlSettings
      .filter { k =>
        Option(k.kcqlConfig.getStoredAs).map(_.toUpperCase).contains("SORTEDSET") &&
          k.kcqlConfig.getTarget != null &&
          k.kcqlConfig.getPrimaryKeys.isEmpty
      }
  )

  /**
    * Construct a RedisSinkSettings object containing all the kcqlConfigs that use the PubSub mode.
    * This function will filter by the presence of the "STOREAS" keyword and a target, as well as the absence of primary keys.
    *
    * KCQL Example: SELECT * FROM cpuTopic STOREAS PubSub (channel=channel)
    *
    * @param settings The RedisSinkSettings containing all kcqlConfigs.
    * @return A RedisSinkSettings object containing only the kcqlConfigs that use the PubSub mode.
    */
  def filterModePubSub(settings: RedisSinkSettings): RedisSinkSettings = settings.copy(kcqlSettings =
    settings.kcqlSettings
      .filter { k =>
        Option(k.kcqlConfig.getStoredAs).map(_.toUpperCase).contains("PUBSUB") &&
          k.kcqlConfig.getPrimaryKeys.isEmpty
      }
  )

  /**
    * Constructs a RedisSinkSettings object containing all the kcqlConfigs that use the Multiple Sorted Sets mode.
    * This function will filter by the presence of the "STOREAS" keyword and the presence of primary keys.
    *
    * KCQL Example: SELECT temperature, humidity FROM sensorsTopic PK sensorID STOREAS SortedSet(score=timestamp)
    *
    * @param settings The RedisSinkSettings containing all kcqlConfigs.
    * @return A RedisSinkSettings object containing only the kcqlConfigs that use the Multiple Sorted Sets mode.
    */
  def filterModePKSS(settings: RedisSinkSettings): RedisSinkSettings = settings.copy(kcqlSettings =
    settings.kcqlSettings
      .filter { k =>
        Option(k.kcqlConfig.getStoredAs).map(_.toUpperCase).contains("SORTEDSET") &&
          k.kcqlConfig.getPrimaryKeys.asScala.length >= 1
      }
  )

  /**
    * Constructs a RedisSinkSettings object containing all the kcqlConfigs that use the Geo Add mode.
    * This function will filter by the presence of the "STOREAS" keyword and the presence of primary keys.
    *
    * KCQL Example: SELECT town, country, longitude, latitude FROM addressTopic PK country
    * STOREAS GeoAdd (longitudeField=longitude,latitudeField=latitude)
    *
    * @param settings The RedisSinkSettings containing all kcqlConfigs.
    * @return A RedisSinkSettings object containing only the kcqlConfigs that use the Geo Add mode.
    */
  def filterGeoAddMode(settings: RedisSinkSettings): RedisSinkSettings = settings.copy(kcqlSettings =
    settings.kcqlSettings
      .filter { k =>
        Option(k.kcqlConfig.getStoredAs).map(_.toUpperCase).contains("GEOADD") &&
          k.kcqlConfig.getPrimaryKeys.size() >= 1
      }
  )

  def filterStream(settings: RedisSinkSettings): RedisSinkSettings = settings.copy(kcqlSettings =
    settings.kcqlSettings
      .filter { k =>
        Option(k.kcqlConfig.getStoredAs).map(_.toUpperCase).contains("STREAM") &&
          k.kcqlConfig.getPrimaryKeys.size() >= 1
      }
  )

  def filterSearch(settings: RedisSinkSettings): RedisSinkSettings = settings.copy(kcqlSettings =
    settings.kcqlSettings
      .filter { k =>
        Option(k.kcqlConfig.getStoredAs).map(_.toUpperCase).contains("SEARCH") &&
          k.kcqlConfig.getPrimaryKeys.size() >= 1
      }
  )

  /**
    * Pass the SinkRecords to the writer for Writing
    **/
  override def put(records: util.Collection[SinkRecord]): Unit = {
    if (records.isEmpty) {
      logger.info("Empty list of records received.")
    }
    else {
      require(writer.nonEmpty, "Writer is not set!")
      val seq = records.asScala.toVector
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
