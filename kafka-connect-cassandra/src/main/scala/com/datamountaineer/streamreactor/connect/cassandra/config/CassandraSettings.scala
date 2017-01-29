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

package com.datamountaineer.streamreactor.connect.cassandra.config

import java.lang.Boolean
import java.net.ConnectException

import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ErrorPolicyEnum, ThrowErrorPolicy}
import com.datastax.driver.core.ConsistencyLevel
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.{AbstractConfig, ConfigException}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
  * Created by andrew@datamountaineer.com on 22/04/16. 
  * stream-reactor
  */

trait CassandraSetting

case class CassandraSourceSetting(routes: Config,
                                  keySpace: String,
                                  bulkImportMode: Boolean = true,
                                  timestampColumn: Option[String] = None,
                                  pollInterval: Long = CassandraConfigConstants.DEFAULT_POLL_INTERVAL,
                                  config: AbstractConfig,
                                  consistencyLevel: Option[ConsistencyLevel],
                                  errorPolicy: ErrorPolicy = new ThrowErrorPolicy,
                                  taskRetires: Int = CassandraConfigConstants.NBR_OF_RETIRES_DEFAULT
                                 ) extends CassandraSetting

case class CassandraSinkSetting(keySpace: String,
                                routes: Set[Config],
                                fields: Map[String, Map[String, String]],
                                ignoreField: Map[String, Set[String]],
                                errorPolicy: ErrorPolicy,
                                threadPoolSize: Int,
                                consistencyLevel: Option[ConsistencyLevel],
                                taskRetries: Int = CassandraConfigConstants.NBR_OF_RETIRES_DEFAULT) extends CassandraSetting

/**
  * Cassandra Setting used for both Readers and writers
  * Holds the table, topic, import mode and timestamp columns
  * Import mode and timestamp columns are only applicable for the source.
  **/
object CassandraSettings extends StrictLogging {

  def configureSource(config: AbstractConfig): Set[CassandraSourceSetting] = {
    //get keyspace
    val keySpace = config.getString(CassandraConfigConstants.KEY_SPACE)
    require(!keySpace.isEmpty, CassandraConfigConstants.MISSING_KEY_SPACE_MESSAGE)
    val raw = config.getString(CassandraConfigConstants.SOURCE_KCQL_QUERY)
    require(!raw.isEmpty, s"${CassandraConfigConstants.SOURCE_KCQL_QUERY} is empty.")
    val routes = raw.split(";").map(r => Config.parse(r)).toSet
    val pollInterval = config.getLong(CassandraConfigConstants.POLL_INTERVAL)

    val bulk = config.getString(CassandraConfigConstants.IMPORT_MODE).toLowerCase() match {
      case CassandraConfigConstants.BULK => true
      case CassandraConfigConstants.INCREMENTAL => false
      case e => throw new ConnectException(s"Unsupported import mode $e.")
    }

    val errorPolicyE = ErrorPolicyEnum.withName(config.getString(CassandraConfigConstants.ERROR_POLICY).toUpperCase)
    val errorPolicy = ErrorPolicy(errorPolicyE)
    val timestampCols = routes.map(r => (r.getSource, r.getPrimaryKeys.toList)).toMap

    val consistencyLevel = config.getString(CassandraConfigConstants.CONSISTENCY_LEVEL_CONFIG) match {
      case "" => None
      case other =>
        Try(ConsistencyLevel.valueOf(other)) match {
          case Failure(e) => throw new ConfigException(s"'$other' is not a valid ${CassandraConfigConstants.CONSISTENCY_LEVEL_CONFIG}. Available values are:${ConsistencyLevel.values().map(_.name()).mkString(",")}")
          case Success(cl) => Some(cl)
        }
    }

    routes.map({
      r => {
        val tCols = timestampCols(r.getSource)
        if (!bulk && tCols.size != 1) {
          throw new ConfigException("Only one timestamp column is allowed to be specified in Incremental mode. " +
            s"Received ${tCols.mkString(",")} for source ${r.getSource}")
        }

        CassandraSourceSetting(
          routes = r,
          keySpace = keySpace,
          timestampColumn = if (bulk) None else Some(tCols.head),
          bulkImportMode = bulk,
          pollInterval = pollInterval,
          config = config,
          errorPolicy = errorPolicy,
          consistencyLevel = consistencyLevel
        )
      }
    })
  }

  def configureSink(config: CassandraConfigSink): CassandraSinkSetting = {
    //get keyspace
    val keySpace = config.getString(CassandraConfigConstants.KEY_SPACE)
    require(!keySpace.isEmpty, CassandraConfigConstants.MISSING_KEY_SPACE_MESSAGE)
    val raw = config.getString(CassandraConfigConstants.SINK_KCQL)
    require(!raw.isEmpty, s"${CassandraConfigConstants.SINK_KCQL} is empty.")
    val routes = raw.split(";").map(r => Config.parse(r)).toSet
    val retries = config.getInt(CassandraConfigConstants.NBR_OF_RETRIES)
    val errorPolicyE = ErrorPolicyEnum.withName(config.getString(CassandraConfigConstants.ERROR_POLICY).toUpperCase)
    val errorPolicy = ErrorPolicy(errorPolicyE)

    val fields = routes.map(rm =>
      (rm.getSource, rm.getFieldAlias.map(fa => (fa.getField, fa.getAlias)).toMap)
    ).toMap


    val threadPoolSize: Int = {
      val threads = config.getInt(CassandraConfigConstants.SINK_THREAD_POOL_CONFIG)
      if (threads <= 0) 4 * Runtime.getRuntime.availableProcessors()
      else threads
    }

    val ignoreFields = routes.map(rm => (rm.getSource, rm.getIgnoredField.toSet)).toMap

    val consistencyLevel = config.getString(CassandraConfigConstants.CONSISTENCY_LEVEL_CONFIG) match {
      case "" => None
      case other =>
        Try(ConsistencyLevel.valueOf(other)) match {
          case Failure(e) => throw new ConfigException(s"'$other' is not a valid ${CassandraConfigConstants.CONSISTENCY_LEVEL_CONFIG}. Available values are:${ConsistencyLevel.values().map(_.name()).mkString(",")}")
          case Success(cl) => Some(cl)
        }
    }

    CassandraSinkSetting(keySpace, routes, fields, ignoreFields, errorPolicy, threadPoolSize, consistencyLevel, retries)
  }
}
