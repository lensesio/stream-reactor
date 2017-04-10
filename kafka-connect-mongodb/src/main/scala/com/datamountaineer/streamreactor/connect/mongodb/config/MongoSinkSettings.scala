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

package com.datamountaineer.streamreactor.connect.mongodb.config

import com.datamountaineer.connector.config.{Config, WriteModeEnum}
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ErrorPolicyEnum}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.{AbstractConfig, ConfigException}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}


case class MongoSinkSettings(connection: String,
                             database: String,
                             kcql: Seq[Config],
                             keyBuilderMap: Map[String, Set[String]],
                             fields: Map[String, Map[String, String]],
                             ignoredField: Map[String, Set[String]],
                             errorPolicy: ErrorPolicy,
                             taskRetries: Int = MongoSinkConfigConstants.NBR_OF_RETIRES_DEFAULT,
                             batchSize: Int = MongoSinkConfigConstants.BATCH_SIZE_CONFIG_DEFAULT)


object MongoSinkSettings extends StrictLogging {

  def apply(config: AbstractConfig): MongoSinkSettings = {
    val hostsConfig = config.getString(MongoSinkConfigConstants.CONNECTION_CONFIG)
    require(hostsConfig.nonEmpty, s"Invalid hosts provided.${MongoSinkConfigConstants.CONNECTION_CONFIG_DOC}")

    val database = config.getString(MongoSinkConfigConstants.DATABASE_CONFIG)
    if (database == null || database.trim.length == 0)
      throw new ConfigException(s"Invalid ${MongoSinkConfigConstants.DATABASE_CONFIG}")
    val kcql = config.getString(MongoSinkConfigConstants.KCQL_CONFIG)
    val routes = kcql.split(";").map(r => Try(Config.parse(r)) match {
      case Success(query) => query
      case Failure(t) => throw new ConfigException(s"Invalid ${MongoSinkConfigConstants.KCQL_CONFIG}.${t.getMessage}", t)
    })
    if (routes.isEmpty)
      throw new ConfigException(s"Invalid ${MongoSinkConfigConstants.KCQL_CONFIG}. You need to provide at least one route")

    val batchSize = config.getInt(MongoSinkConfigConstants.BATCH_SIZE_CONFIG)
    val errorPolicyE = ErrorPolicyEnum.withName(config.getString(MongoSinkConfigConstants.ERROR_POLICY_CONFIG).toUpperCase)
    val errorPolicy = ErrorPolicy(errorPolicyE)
    val retries = config.getInt(MongoSinkConfigConstants.NBR_OF_RETRIES_CONFIG)

    val rowKeyBuilderMap = routes
      .filter(c => c.getWriteMode == WriteModeEnum.UPSERT)
      .map { r =>
        val keys = r.getPrimaryKeys.toSet
        if (keys.isEmpty) throw new ConfigException(s"${r.getTarget} is set up with upsert, you need primary keys setup")
        (r.getSource, keys)
      }.toMap


    val fieldsMap = routes.map { rm =>
      (rm.getSource, rm.getFieldAlias.map(fa => (fa.getField, fa.getAlias)).toMap)
    }.toMap

    val ignoreFields = routes.map(r => (r.getSource, r.getIgnoredField.toSet)).toMap

    new MongoSinkSettings(hostsConfig,
      database,
      routes,
      rowKeyBuilderMap,
      fieldsMap,
      ignoreFields,
      errorPolicy,
      retries,
      batchSize)
  }
}
