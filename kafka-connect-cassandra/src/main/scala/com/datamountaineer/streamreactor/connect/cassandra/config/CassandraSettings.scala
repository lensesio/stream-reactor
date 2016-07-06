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

package com.datamountaineer.streamreactor.connect.cassandra.config

import java.lang.Boolean
import java.net.ConnectException

import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ErrorPolicyEnum, ThrowErrorPolicy}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.{AbstractConfig, ConfigException}

import scala.collection.JavaConverters._
/**
  * Created by andrew@datamountaineer.com on 22/04/16. 
  * stream-reactor
  */

trait CassandraSetting

case class CassandraSourceSetting(routes : Config,
                                  keySpace: String,
                                  bulkImportMode: Boolean = true,
                                  timestampColumn: Option[String] = None,
                                  pollInterval : Long = CassandraConfigConstants.DEFAULT_POLL_INTERVAL,
                                  config: AbstractConfig,
                                  errorPolicy : ErrorPolicy = new ThrowErrorPolicy,
                                  taskRetires : Int = CassandraConfigConstants.NBR_OF_RETIRES_DEFAULT
                               ) extends CassandraSetting

case class CassandraSinkSetting(keySpace: String,
                                routes: Set[Config],
                                fields : Map[String, Map[String, String]],
                                ignoreField : Map[String, Set[String]],
                                errorPolicy: ErrorPolicy,
                                taskRetries: Int = CassandraConfigConstants.NBR_OF_RETIRES_DEFAULT) extends CassandraSetting

/**
  * Cassandra Setting used for both Readers and writers
  * Holds the table, topic, import mode and timestamp columns
  * Import mode and timestamp columns are only applicable for the source.
  * */
object CassandraSettings extends StrictLogging {

  def configureSource(config: AbstractConfig, assigned: List[String]): Set[CassandraSourceSetting] = {
    //get keyspace
    val keySpace = config.getString(CassandraConfigConstants.KEY_SPACE)
    require(!keySpace.isEmpty, CassandraConfigConstants.MISSING_KEY_SPACE_MESSAGE)
    val raw = config.getString(CassandraConfigConstants.IMPORT_ROUTE_QUERY)
    require(!raw.isEmpty, s"${CassandraConfigConstants.IMPORT_ROUTE_QUERY} is empty.")
    val routes = raw.split(";").map(r => Config.parse(r)).toSet
    val pollInterval = config.getLong(CassandraConfigConstants.POLL_INTERVAL)

    val bulk = config.getString(CassandraConfigConstants.IMPORT_MODE).toLowerCase() match {
      case CassandraConfigConstants.BULK => true
      case CassandraConfigConstants.INCREMENTAL => false
      case _@e => throw new ConnectException(s"Unsupported import mode $e.")
    }

    val errorPolicyE = ErrorPolicyEnum.withName(config.getString(CassandraConfigConstants.ERROR_POLICY).toUpperCase)
    val errorPolicy = ErrorPolicy(errorPolicyE)
    val timestampCols = routes.map(r=>(r.getSource, r.getPrimaryKeys.asScala.toList)).toMap

    routes.map({
      r => {
        val tCols = timestampCols.get(r.getSource).get
        if (!bulk && tCols.size != 1) {
          throw new ConfigException("Only one timestamp column is allowed to be specified in Incremental mode. " +
            s"Received ${tCols.mkString(",")} for source ${r.getSource}")
        }

        new CassandraSourceSetting(
          routes = r,
          keySpace = keySpace,
          timestampColumn = if (bulk) None else Some(tCols.head),
          bulkImportMode = bulk,
          pollInterval = pollInterval,
          config = config,
          errorPolicy = errorPolicy
        )
      }
    })
  }

  def configureSink(config: CassandraConfigSink): CassandraSinkSetting = {
    //get keyspace
    val keySpace = config.getString(CassandraConfigConstants.KEY_SPACE)
    require(!keySpace.isEmpty, CassandraConfigConstants.MISSING_KEY_SPACE_MESSAGE)
    val raw = config.getString(CassandraConfigConstants.EXPORT_ROUTE_QUERY)
    require(!raw.isEmpty, s"${CassandraConfigConstants.EXPORT_ROUTE_QUERY} is empty.")
    val routes = raw.split(";").map(r => Config.parse(r)).toSet
    val retries = config.getInt(CassandraConfigConstants.NBR_OF_RETRIES)
    val errorPolicyE = ErrorPolicyEnum.withName(config.getString(CassandraConfigConstants.ERROR_POLICY).toUpperCase)
    val errorPolicy = ErrorPolicy(errorPolicyE)

    val fields = routes.map(rm =>
      (rm.getSource, rm.getFieldAlias.asScala.map(fa => (fa.getField,fa.getAlias)).toMap)
    ).toMap

    val ignoreFields = routes.map(rm => (rm.getSource, rm.getIgnoredField.asScala.toSet)).toMap
    CassandraSinkSetting(keySpace, routes, fields, ignoreFields, errorPolicy, retries)
  }
}
