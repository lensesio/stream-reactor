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

import java.util

import com.datamountaineer.streamreactor.connect.config.base.traits._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

object MongoConfig {

  val config = new ConfigDef()
    .define(MongoConfigConstants.CONNECTION_CONFIG, Type.STRING, Importance.HIGH,
      MongoConfigConstants.CONNECTION_CONFIG_DOC, "Connection", 1, ConfigDef.Width.LONG,
      MongoConfigConstants.CONNECTION_CONFIG)
    .define(MongoConfigConstants.DATABASE_CONFIG, Type.STRING, Importance.HIGH,
      MongoConfigConstants.DATABASE_CONFIG_DOC, "Connection", 2, ConfigDef.Width.MEDIUM,
      MongoConfigConstants.DATABASE_CONFIG)


    .define(MongoConfigConstants.USERNAME_CONFIG, Type.STRING, MongoConfigConstants.USERNAME_CONFIG_DEFAULT,
      Importance.MEDIUM, MongoConfigConstants.USERNAME_CONFIG_DOC, "Connection", 3, ConfigDef.Width.MEDIUM,
      MongoConfigConstants.USERNAME_CONFIG)
    .define(MongoConfigConstants.PASSWORD_CONFIG, Type.PASSWORD, MongoConfigConstants.PASSWORD_CONFIG_DEFAULT,
      Importance.MEDIUM, MongoConfigConstants.PASSWORD_CONFIG_DOC, "Connection", 4, ConfigDef.Width.MEDIUM,
      MongoConfigConstants.PASSWORD_CONFIG)
    .define(MongoConfigConstants.AUTHENTICATION_MECHANISM, Type.STRING, MongoConfigConstants.AUTHENTICATION_MECHANISM_DEFAULT,
      Importance.MEDIUM, MongoConfigConstants.AUTHENTICATION_MECHANISM_DOC, "Connection", 5, ConfigDef.Width.MEDIUM,
      MongoConfigConstants.AUTHENTICATION_MECHANISM)

    .define(MongoConfigConstants.KCQL_CONFIG, Type.STRING, Importance.HIGH, MongoConfigConstants.KCQL_DOC,
      "Mappings", 1, ConfigDef.Width.LONG, MongoConfigConstants.KCQL_CONFIG)
    .define(MongoConfigConstants.ERROR_POLICY_CONFIG, Type.STRING, MongoConfigConstants.ERROR_POLICY_DEFAULT,
      Importance.HIGH, MongoConfigConstants.ERROR_POLICY_DOC, "Error", 1, ConfigDef.Width.LONG,
      MongoConfigConstants.ERROR_POLICY_CONFIG)
    .define(MongoConfigConstants.NBR_OF_RETRIES_CONFIG, Type.INT, MongoConfigConstants.NBR_OF_RETIRES_DEFAULT,
      Importance.MEDIUM, MongoConfigConstants.NBR_OF_RETRIES_DOC, "Error", 2, ConfigDef.Width.LONG,
      MongoConfigConstants.NBR_OF_RETRIES_CONFIG)
    .define(MongoConfigConstants.ERROR_RETRY_INTERVAL_CONFIG, Type.INT,
      MongoConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT, Importance.MEDIUM,
      MongoConfigConstants.ERROR_RETRY_INTERVAL_DOC, "Error", 3, ConfigDef.Width.LONG,
      MongoConfigConstants.ERROR_RETRY_INTERVAL_CONFIG)
    .define(MongoConfigConstants.PROGRESS_COUNTER_ENABLED, Type.BOOLEAN,
      MongoConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
      Importance.MEDIUM, MongoConfigConstants.PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics", 1, ConfigDef.Width.MEDIUM, MongoConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY)
}


case class MongoConfig(props: util.Map[String, String])
  extends BaseConfig(MongoConfigConstants.CONNECTOR_PREFIX, MongoConfig.config, props)
  with KcqlSettings
  with DatabaseSettings
  with ErrorPolicySettings
  with NumberRetriesSettings
  with UserSettings
