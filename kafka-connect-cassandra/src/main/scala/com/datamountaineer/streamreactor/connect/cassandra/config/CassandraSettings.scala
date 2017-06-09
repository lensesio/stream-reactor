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

package com.datamountaineer.streamreactor.connect.cassandra.config

import java.lang.Boolean

import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.cassandra.config.TimestampType.TimestampType
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ThrowErrorPolicy}
import com.datastax.driver.core.ConsistencyLevel
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigException

import scala.collection.JavaConversions.asScalaIterator
import scala.util.{Success, Try}

/**
  * Created by andrew@datamountaineer.com on 22/04/16. 
  * stream-reactor
  */

trait CassandraSetting

object TimestampType extends Enumeration {
  type TimestampType = Value
  val TIMESTAMP, TIMEUUID, TOKEN, NONE = Value
}

case class CassandraSourceSetting(routes: Config,
                                  keySpace: String,
                                  primaryKeyColumn: Option[String] = None,
                                  timestampColType: TimestampType,
                                  pollInterval: Long = CassandraConfigConstants.DEFAULT_POLL_INTERVAL,
                                  consistencyLevel: Option[ConsistencyLevel],
                                  errorPolicy: ErrorPolicy = new ThrowErrorPolicy,
                                  taskRetires: Int = CassandraConfigConstants.NBR_OF_RETIRES_DEFAULT,
                                  fetchSize: Int = CassandraConfigConstants.FETCH_SIZE_DEFAULT
                                 ) extends CassandraSetting

case class CassandraSinkSetting(keySpace: String,
                                routes: Set[Config],
                                fields: Map[String, Map[String, String]],
                                ignoreField: Map[String, Set[String]],
                                errorPolicy: ErrorPolicy,
                                threadPoolSize: Int,
                                consistencyLevel: Option[ConsistencyLevel],
                                taskRetries: Int = CassandraConfigConstants.NBR_OF_RETIRES_DEFAULT,
                                enableProgress: Boolean = CassandraConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT) extends CassandraSetting

/**
  * Cassandra Setting used for both Readers and writers
  * Holds the table, topic, import mode and timestamp columns
  * Import mode and timestamp columns are only applicable for the source.
  **/
object CassandraSettings extends StrictLogging {

  def configureSource(config: CassandraConfigSource): Set[CassandraSourceSetting] = {
    //get keyspace
    val keySpace = config.getString(CassandraConfigConstants.KEY_SPACE)
    require(!keySpace.isEmpty, CassandraConfigConstants.MISSING_KEY_SPACE_MESSAGE)
    val pollInterval = config.getLong(CassandraConfigConstants.POLL_INTERVAL)

    val consistencyLevel = config.getConsistencyLevel
    val errorPolicy = config.getErrorPolicy
    val routes = config.getRoutes
    val primaryKeyCols = routes.map(r => (r.getSource, r.getPrimaryKeys.toList)).toMap
    val fetchSize = config.getInt(CassandraConfigConstants.FETCH_SIZE)
    val incrementalModes = config.getIncrementalMode(routes)

    routes.map({
      r => {
        val tCols = primaryKeyCols(r.getSource)
        val timestampType = Try(TimestampType.withName(incrementalModes.get(r.getSource).get.toUpperCase)) match {
          case Success(s) => s
          case _ => TimestampType.NONE
        }

        if (tCols.size != 1 && TimestampType.equals(TimestampType.NONE)) {
          throw new ConfigException("Only one primary key column is allowed to be specified in Incremental mode. " +
            s"Received ${tCols.mkString(",")} for source ${r.getSource}")
        }

        CassandraSourceSetting(
          routes = r,
          keySpace = keySpace,
          primaryKeyColumn = if (tCols.isEmpty) None else Some(tCols.head),
          timestampColType = timestampType,
          pollInterval = pollInterval,
          errorPolicy = errorPolicy,
          consistencyLevel = consistencyLevel,
          fetchSize = fetchSize
        )
      }
    })
  }

  def configureSink(config: CassandraConfigSink): CassandraSinkSetting = {
    //get keyspace
    val keySpace = config.getString(CassandraConfigConstants.KEY_SPACE)
    require(!keySpace.isEmpty, CassandraConfigConstants.MISSING_KEY_SPACE_MESSAGE)
    val errorPolicy = config.getErrorPolicy
    val retries = config.getNumberRetries
    val routes: Set[Config] = config.getRoutes
    val fields = config.getFields(routes)
    val ignoreFields = config.getIgnoreFields(routes)
    val threadPoolSize = config.getThreadPoolSize
    val consistencyLevel = config.getConsistencyLevel

    val enableCounter = config.getBoolean(CassandraConfigConstants.PROGRESS_COUNTER_ENABLED)
    CassandraSinkSetting(keySpace,
      routes,
      fields,
      ignoreFields,
      errorPolicy,
      threadPoolSize,
      consistencyLevel,
      retries,
      enableCounter)
  }
}
