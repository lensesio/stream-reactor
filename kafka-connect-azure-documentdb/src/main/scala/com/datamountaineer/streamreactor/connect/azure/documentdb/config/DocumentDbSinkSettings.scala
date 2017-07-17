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

package com.datamountaineer.streamreactor.connect.azure.documentdb.config

import com.datamountaineer.connector.config.{Config, WriteModeEnum}
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ErrorPolicyEnum}
import com.microsoft.azure.documentdb.ConsistencyLevel
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.{AbstractConfig, ConfigException}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}


case class DocumentDbSinkSettings(endpoint: String,
                                  masterKey: String,
                                  database: String,
                                  kcql: Seq[Config],
                                  keyBuilderMap: Map[String, Set[String]],
                                  fields: Map[String, Map[String, String]],
                                  ignoredField: Map[String, Set[String]],
                                  errorPolicy: ErrorPolicy,
                                  consistency: ConsistencyLevel,
                                  createDatabase: Boolean,
                                  proxy: Option[String],
                                  taskRetries: Int = DocumentDbConfigConstants.NBR_OF_RETIRES_DEFAULT
                                  ) {

}


object DocumentDbSinkSettings extends StrictLogging {

  def apply(config: DocumentDbConfig): DocumentDbSinkSettings = {
    val endpoint = config.getString(DocumentDbConfigConstants.CONNECTION_CONFIG)
    require(endpoint.nonEmpty, s"Invalid endpoint provided.${DocumentDbConfigConstants.CONNECTION_CONFIG_DOC}")

    val masterKey = Option(config.getPassword(DocumentDbConfigConstants.MASTER_KEY_CONFIG))
      .map(_.value())
      .getOrElse(throw new ConfigException(s"Missing ${DocumentDbConfigConstants.MASTER_KEY_CONFIG}"))
    require(masterKey.trim.nonEmpty, s"Invalid ${DocumentDbConfigConstants.MASTER_KEY_CONFIG}")

    val database = config.getDatabase

    if (database.isEmpty) {
      throw new ConfigException(s"Missing ${DocumentDbConfigConstants.DATABASE_CONFIG}.")
    }

    val kcql = config.getKCQL
    val errorPolicy= config.getErrorPolicy
    val retries = config.getRetryInterval
    val rowKeyBuilderMap = config.getUpsertKeys(kcql)
    val fieldsMap = config.getFields(kcql)
    val ignoreFields = config.getIgnoreFields(kcql)

    val consistencyLevel = Try(ConsistencyLevel.valueOf(config.getString(DocumentDbConfigConstants.CONSISTENCY_CONFIG))) match {
      case Failure(_) => throw new ConfigException(
        s"""
           |${config.getString(DocumentDbConfigConstants.CONSISTENCY_CONFIG)} is not a valid entry for ${DocumentDbConfigConstants.CONSISTENCY_CONFIG}
           |Available values are ${ConsistencyLevel.values().mkString(",")}""".stripMargin)

      case Success(c) => c
    }

    new DocumentDbSinkSettings(endpoint,
      masterKey,
      database,
      kcql.toSeq,
      rowKeyBuilderMap,
      fieldsMap,
      ignoreFields,
      errorPolicy,
      consistencyLevel,
      config.getBoolean(DocumentDbConfigConstants.CREATE_DATABASE_CONFIG),
      Option(config.getString(DocumentDbConfigConstants.PROXY_HOST_CONFIG)),
      retries)
  }
}
