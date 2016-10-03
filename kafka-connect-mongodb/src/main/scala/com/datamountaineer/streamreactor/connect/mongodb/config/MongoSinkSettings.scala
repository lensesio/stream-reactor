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

package com.datamountaineer.streamreactor.connect.mongodb.config

import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ErrorPolicyEnum}
import com.datamountaineer.streamreactor.connect.rowkeys.{StringGenericRowKeyBuilder, StringKeyBuilder, StringStructFieldsStringKeyBuilder}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.{AbstractConfig, ConfigException}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}


case class MongoSinkSettings(hosts: Seq[String],
                             database: String,
                             routes: Seq[Config],
                             keyBuilderMap: Map[String, StringKeyBuilder],
                             fields: Map[String, Map[String, String]],
                             ignoreField: Map[String, Set[String]],
                             errorPolicy: ErrorPolicy,
                             taskRetries: Int = MongoConfig.NBR_OF_RETIRES_DEFAULT,
                             batchSize: Int = MongoConfig.BATCH_SIZE_CONFIG_DEFAULT)


object MongoSinkSettings extends StrictLogging {

  def apply(config: AbstractConfig): MongoSinkSettings = {
    val hostsConfig = config.getString(MongoConfig.HOSTS_CONFIG)
    require(hostsConfig.nonEmpty, s"Invalid hosts provided.${MongoConfig.HOSTS_CONFIG_DOC}")

    val hosts = hostsConfig.split(",").map(_.trim).map { h =>
      h.split(":") match {
        case Array(hostname: String, port: String) if Try(port.toInt).isSuccess =>
        case _ => throw new ConfigException(s"Invalid ${MongoConfig.HOSTS_CONFIG}. ${MongoConfig.HOSTS_CONFIG_DOC}")
      }
      h
    }
    val database = config.getString(MongoConfig.DATABASE_CONFIG)
    if (database == null || database.trim.length == 0)
      throw new ConfigException(s"Invalid ${MongoConfig.DATABASE_CONFIG}")
    val kcql = config.getString(MongoConfig.KCQL_CONFIG)
    val routes = kcql.split(";").map(r => Try(Config.parse(r)) match {
      case Success(query) => query
      case Failure(t) => throw new ConfigException(s"Invalid ${MongoConfig.KCQL_CONFIG}.${t.getMessage}", t)
    })
    if (routes.isEmpty)
      throw new ConfigException(s"Invalid ${MongoConfig.KCQL_CONFIG}. You need to provide at least one route")

    val batchSize = config.getInt(MongoConfig.BATCH_SIZE_CONFIG)
    val errorPolicyE = ErrorPolicyEnum.withName(config.getString(MongoConfig.ERROR_POLICY_CONFIG).toUpperCase)
    val errorPolicy = ErrorPolicy(errorPolicyE)
    val retries = config.getInt(MongoConfig.NBR_OF_RETRIES_CONFIG)

    val rowKeyBuilderMap = routes.map { r =>
      val keys = r.getPrimaryKeys.toList
      if (keys.nonEmpty) (r.getSource, StringStructFieldsStringKeyBuilder(keys))
      else (r.getSource, new StringGenericRowKeyBuilder())
    }.toMap


    val fieldsMap = routes.map { rm =>
      (rm.getSource, rm.getFieldAlias.map(fa => (fa.getField, fa.getAlias)).toMap)
    }.toMap

    val ignoreFields = routes.map(r => (r.getSource, r.getIgnoredField.toSet)).toMap

    new MongoSinkSettings(hosts,
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
