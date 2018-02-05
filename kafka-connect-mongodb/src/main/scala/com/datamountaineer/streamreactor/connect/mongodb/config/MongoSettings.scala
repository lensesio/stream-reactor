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

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.errors.ErrorPolicy
import com.mongodb.AuthenticationMechanism
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.types.Password


case class MongoSettings(connection: String,
                         username: String,
                         password: Password,
                         authenticationMechanism: AuthenticationMechanism,
                         database: String,
                         kcql: Set[Kcql],
                         keyBuilderMap: Map[String, Set[String]],
                         fields: Map[String, Map[String, String]],
                         ignoredField: Map[String, Set[String]],
                         errorPolicy: ErrorPolicy,
                         taskRetries: Int = MongoConfigConstants.NBR_OF_RETIRES_DEFAULT)


object MongoSettings extends StrictLogging {

  def apply(config: MongoConfig): MongoSettings = {
    val hostsConfig = config.getString(MongoConfigConstants.CONNECTION_CONFIG)
    require(hostsConfig.nonEmpty, s"Invalid hosts provided.${MongoConfigConstants.CONNECTION_CONFIG_DOC}")

    val database = config.getDatabase

    if (database.contains("-")) {
      throw new ConfigException(s"${MongoConfigConstants.DATABASE_CONFIG} contains an '-' which are invalid characters for mongo collections")
    }

    require(database.nonEmpty, s"${MongoConfigConstants.DATABASE_CONFIG} is empty")

    val kcql = config.getKCQL
    val errorPolicy= config.getErrorPolicy
    val retries = config.getNumberRetries
    val rowKeyBuilderMap = config.getUpsertKeys()
    val fieldsMap = config.getFieldsMap(kcql)
    val ignoreFields = config.getIgnoreFieldsMap()

    val username = config.getUsername
    val password = config.getSecret

    val authenticationMechanism = AuthenticationMechanism.fromMechanismName(config.getString(MongoConfigConstants.AUTHENTICATION_MECHANISM))

    new MongoSettings(
        hostsConfig,
        username,
        password,
        authenticationMechanism,
        database,
        kcql,
        rowKeyBuilderMap,
        fieldsMap,
        ignoreFields,
        errorPolicy,
        retries
    )
  }
}
