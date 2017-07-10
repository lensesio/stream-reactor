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

package com.datamountaineer.streamreactor.connect.rethink.config

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

/**
  * Created by andrew@datamountaineer.com on 10/07/2017. 
  * stream-reactor
  */
trait ReThinkConfig {
  val baseConfig: ConfigDef = new ConfigDef()
    .define(ReThinkConfigConstants.RETHINK_HOST, Type.STRING,
      ReThinkConfigConstants.RETHINK_HOST_DEFAULT,
      Importance.HIGH, ReThinkConfigConstants.RETHINK_HOST_DOC,
      "Connection", 1, ConfigDef.Width.MEDIUM, ReThinkConfigConstants.RETHINK_HOST)

    .define(ReThinkConfigConstants.RETHINK_DB, Type.STRING,
      ReThinkConfigConstants.RETHINK_DB_DEFAULT,
      Importance.HIGH, ReThinkConfigConstants.RETHINK_DB_DOC,
      "Connection", 2, ConfigDef.Width.MEDIUM, ReThinkConfigConstants.RETHINK_DB)

    .define(ReThinkConfigConstants.RETHINK_PORT, Type.INT,
      ReThinkConfigConstants.RETHINK_PORT_DEFAULT,
      Importance.MEDIUM, ReThinkConfigConstants.RETHINK_PORT_DOC,
      "Connection", 3, ConfigDef.Width.MEDIUM, ReThinkConfigConstants.RETHINK_PORT)

    .define(ReThinkConfigConstants.KCQL, Type.STRING, Importance.HIGH,
      ReThinkConfigConstants.KCQL_DOC,
      "Connection", 4, ConfigDef.Width.MEDIUM, ReThinkConfigConstants.KCQL)

    .define(ReThinkConfigConstants.USERNAME, Type.STRING,
      ReThinkConfigConstants.USERNAME_DEFAULT,
      Importance.MEDIUM, ReThinkConfigConstants.USERNAME_DOC,
      "Connection", 5, ConfigDef.Width.MEDIUM, ReThinkConfigConstants.USERNAME)

    .define(ReThinkConfigConstants.PASSWORD, Type.PASSWORD,
      ReThinkConfigConstants.PASSWORD_DEFAULT,
      Importance.MEDIUM, ReThinkConfigConstants.PASSWORD_DOC,
      "Connection", 6, ConfigDef.Width.MEDIUM, ReThinkConfigConstants.PASSWORD)

    .define(ReThinkConfigConstants.AUTH_KEY, Type.PASSWORD,
      ReThinkConfigConstants.AUTH_KEY_DEFAULT,
      Importance.MEDIUM, ReThinkConfigConstants.AUTH_KEY_DOC,
      "Connection", 7, ConfigDef.Width.MEDIUM, ReThinkConfigConstants.AUTH_KEY)

    .define(ReThinkConfigConstants.CERT_FILE, Type.STRING,
      ReThinkConfigConstants.CERT_FILE_DEFAULT,
      Importance.MEDIUM, ReThinkConfigConstants.CERT_FILE_DOC,
      "Connection", 8, ConfigDef.Width.MEDIUM, ReThinkConfigConstants.CERT_FILE)

    .define(ReThinkConfigConstants.PROGRESS_COUNTER_ENABLED, Type.BOOLEAN,
      ReThinkConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT, Importance.MEDIUM,
      ReThinkConfigConstants.PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics", 1, ConfigDef.Width.MEDIUM, ReThinkConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY)
}
