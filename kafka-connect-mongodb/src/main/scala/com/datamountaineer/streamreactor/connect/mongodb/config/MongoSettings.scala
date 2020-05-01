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
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.config.SslConfigs
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
                         taskRetries: Int = MongoConfigConstants.NBR_OF_RETIRES_DEFAULT,
                         // Set of field name lists:
                         jsonDateTimeFields: Set[Seq[String]] = Set.empty,
                         trustStoreType: Option[String] = None,
                         trustStorePassword: Option[String] = None,
                         trustStoreLocation: Option[String] = None,
                         keyStoreType: Option[String] = None,
                         keyStorePassword: Option[String] = None,
                         keyStoreLocation: Option[String] = None
                        )


object MongoSettings extends StrictLogging {

  def apply(config: MongoConfig): MongoSettings = {
    val hostsConfig = config.getString(MongoConfigConstants.CONNECTION_CONFIG)
    require(hostsConfig.nonEmpty, s"Invalid hosts provided.${MongoConfigConstants.CONNECTION_CONFIG_DOC}")

    val database = config.getDatabase

    require(database.nonEmpty, s"${MongoConfigConstants.DATABASE_CONFIG} is empty")

    val kcql = config.getKCQL
    val errorPolicy= config.getErrorPolicy
    val retries = config.getNumberRetries
    val rowKeyBuilderMap = config.getUpsertKeys(preserveFullKeys=true)
    val fieldsMap = config.getFieldsMap(kcql)
    val ignoreFields = config.getIgnoreFieldsMap()

    val username = config.getUsername
    val password = config.getSecret

    val trustStoreType = Option(config.getString(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG))
    val trustStorePath = Option(config.getString(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG))
    val trustStorePassword = Option(config.getPassword(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)) match {
      case Some(p) => Some(p.value())
      case None => None
    }

    val keyStoreType = Option(config.getString(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG))
    val keyStorePath = Option(config.getString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
    val keyStorePassword = Option(config.getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)) match {
      case Some(p) => Some(p.value())
      case None => None
    }

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
        retries,
        getJsonDateTimeFields(config),
        trustStoreType,
        trustStorePassword,
        trustStorePath,
        keyStoreType,
        keyStorePassword,
        keyStorePath
    )
  }

  /**
    * Parse out the jsonDateTimeFields list into the structure we need, which is
    * a Set of field 'paths'; ie. :
    *    Set(
    *      Seq("top-level-field"),
    *      Seq("top-level-parent", "child1", "child2", "fieldname"),
    *    )
    */
  def getJsonDateTimeFields(config: MongoConfig): Set[Seq[String]] = {
    import scala.collection.JavaConverters._
    val set: Set[Seq[String]] =
      config.getList(MongoConfigConstants.JSON_DATETIME_FIELDS_CONFIG).
        asScala.
        map{ fullName =>
          fullName.trim.split('.').toSeq
        }.toSet
    logger.info(s"MongoConfigConstants.JSON_DATETIME_FIELDS_CONFIG is $set")
    set
  }
}
