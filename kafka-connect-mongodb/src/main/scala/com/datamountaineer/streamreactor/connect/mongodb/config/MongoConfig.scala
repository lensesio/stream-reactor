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

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

object MongoConfig {

  val configDef: ConfigDef = new ConfigDef()
    .define(MongoSinkConfigConstants.CONNECTION_CONFIG, Type.STRING, Importance.HIGH, MongoSinkConfigConstants.CONNECTION_CONFIG_DOC, "Connection", 1, ConfigDef.Width.LONG, MongoSinkConfigConstants.CONNECTION_CONFIG)
    .define(MongoSinkConfigConstants.DATABASE_CONFIG, Type.STRING, Importance.HIGH, MongoSinkConfigConstants.DATABASE_CONFIG_DOC, "Connection", 2, ConfigDef.Width.MEDIUM, MongoSinkConfigConstants.DATABASE_CONFIG)
    .define(MongoSinkConfigConstants.KCQL_CONFIG, Type.STRING, Importance.HIGH, MongoSinkConfigConstants.KCQL_DOC, "Mappings", 1, ConfigDef.Width.LONG, MongoSinkConfigConstants.KCQL_CONFIG)
    .define(MongoSinkConfigConstants.BATCH_SIZE_CONFIG, Type.INT, MongoSinkConfigConstants.BATCH_SIZE_CONFIG_DEFAULT, Importance.MEDIUM, MongoSinkConfigConstants.BATCH_SIZE_DOC, "Mappings", 2, ConfigDef.Width.MEDIUM, MongoSinkConfigConstants.BATCH_SIZE_CONFIG)
    .define(MongoSinkConfigConstants.ERROR_POLICY_CONFIG, Type.STRING, MongoSinkConfigConstants.ERROR_POLICY_DEFAULT, Importance.HIGH, MongoSinkConfigConstants.ERROR_POLICY_DOC, "Error", 1, ConfigDef.Width.LONG, MongoSinkConfigConstants.ERROR_POLICY_CONFIG)
    .define(MongoSinkConfigConstants.NBR_OF_RETRIES_CONFIG, Type.INT, MongoSinkConfigConstants.NBR_OF_RETIRES_DEFAULT, Importance.MEDIUM, MongoSinkConfigConstants.NBR_OF_RETRIES_DOC, "Error", 2, ConfigDef.Width.LONG, MongoSinkConfigConstants.NBR_OF_RETRIES_CONFIG)
    .define(MongoSinkConfigConstants.ERROR_RETRY_INTERVAL_CONFIG, Type.INT, MongoSinkConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT, Importance.MEDIUM, MongoSinkConfigConstants.ERROR_RETRY_INTERVAL_DOC, "Error", 3, ConfigDef.Width.LONG, MongoSinkConfigConstants.ERROR_RETRY_INTERVAL_CONFIG)
    .define(MongoSinkConfigConstants.PROGRESS_COUNTER_ENABLED, Type.BOOLEAN, MongoSinkConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
      Importance.MEDIUM, MongoSinkConfigConstants.PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics", 1, ConfigDef.Width.MEDIUM, MongoSinkConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY)
}


case class MongoConfig(props: util.Map[String, String]) extends AbstractConfig(MongoConfig.configDef, props)
