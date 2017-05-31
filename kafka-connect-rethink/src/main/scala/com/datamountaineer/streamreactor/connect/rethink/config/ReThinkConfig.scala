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

import com.datamountaineer.streamreactor.connect.coap.configs.CoapConstants
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

/**
  * Created by andrew@datamountaineer.com on 29/05/2017. 
  * stream-reactor
  */
case class ReThinkConfig() {
  val base = new ConfigDef()
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
    .define(ReThinkConfigConstants.USERNAME,
      Type.STRING, "", Importance.LOW, ReThinkConfigConstants.USERNAME_DOC,
      "Connection", 4, ConfigDef.Width.LONG, ReThinkConfigConstants.USERNAME)
    .define(ReThinkConfigConstants.PASSWORD,
      Type.PASSWORD, "", Importance.LOW, ReThinkConfigConstants.PASSWORD_DOC,
      "Connection", 5, ConfigDef.Width.LONG, ReThinkConfigConstants.PASSWORD)

    .define(ReThinkConfigConstants.CERT_FILE, Type.STRING, ReThinkConfigConstants.CERT_FILE_DEFAULT,
      Importance.MEDIUM, ReThinkConfigConstants.CERT_FILE_DOC,
      "Connection", 6, ConfigDef.Width.MEDIUM, ReThinkConfigConstants.CERT_FILE)

    .define(ReThinkConfigConstants.AUTH_KEY, Type.PASSWORD, ReThinkConfigConstants.AUTH_KEY_DEFAULT,
      Importance.MEDIUM, ReThinkConfigConstants.AUTH_KEY_DOC,
      "Connection", 7, ConfigDef.Width.MEDIUM, ReThinkConfigConstants.AUTH_KEY)

    .define(ReThinkConfigConstants.SSL_ENABLED,
      Type.BOOLEAN, ReThinkConfigConstants.SSL_ENABLED_DEFAULT, Importance.LOW, ReThinkConfigConstants.SSL_ENABLED_DOC,
      "Connection", 8, ConfigDef.Width.LONG, ReThinkConfigConstants.SSL_ENABLED)

    .define(ReThinkConfigConstants.TRUST_STORE_PATH,
      Type.STRING, "", Importance.LOW, ReThinkConfigConstants.TRUST_STORE_PATH_DOC,
      "Connection", 9, ConfigDef.Width.LONG, ReThinkConfigConstants.TRUST_STORE_PATH)

    .define(ReThinkConfigConstants.TRUST_STORE_PASSWD,
      Type.PASSWORD, "", Importance.LOW, ReThinkConfigConstants.TRUST_STORE_PASSWD_DOC,
      "Connection", 10, ConfigDef.Width.LONG, ReThinkConfigConstants.TRUST_STORE_PASSWD)

    .define(ReThinkConfigConstants.TRUST_STORE_TYPE, Type.STRING, ReThinkConfigConstants.TRUST_STORE_TYPE_DEFAULT, Importance.MEDIUM,
      ReThinkConfigConstants.TRUST_STORE_TYPE_DOC,
      "Connection", 11, ConfigDef.Width.MEDIUM, ReThinkConfigConstants.TRUST_STORE_TYPE)

    .define(ReThinkConfigConstants.KEY_STORE_TYPE, Type.STRING, ReThinkConfigConstants.KEY_STORE_TYPE_DEFAULT,
      Importance.MEDIUM, ReThinkConfigConstants.KEY_STORE_TYPE_DOC,
      "Connection", 12, ConfigDef.Width.MEDIUM, ReThinkConfigConstants.KEY_STORE_TYPE)

    .define(ReThinkConfigConstants.USE_CLIENT_AUTH, Type.BOOLEAN, ReThinkConfigConstants.USE_CLIENT_AUTH_DEFAULT, Importance.LOW,
      ReThinkConfigConstants.USE_CLIENT_AUTH_DOC,
      "Connection", 13, ConfigDef.Width.LONG, ReThinkConfigConstants.USE_CLIENT_AUTH)

    .define(ReThinkConfigConstants.KEY_STORE_PATH,
      Type.STRING, "", Importance.LOW, ReThinkConfigConstants.KEY_STORE_PATH_DOC,
      "Connection", 14, ConfigDef.Width.LONG, ReThinkConfigConstants.KEY_STORE_PATH)

    .define(ReThinkConfigConstants.KEY_STORE_PASSWD,
      Type.PASSWORD, "", Importance.LOW, ReThinkConfigConstants.KEY_STORE_PASSWD_DOC,
      "Connection", 15, ConfigDef.Width.LONG, ReThinkConfigConstants.KEY_STORE_PASSWD)

    .define(ReThinkConfigConstants.PROGRESS_COUNTER_ENABLED, Type.BOOLEAN, ReThinkConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
      Importance.MEDIUM, ReThinkConfigConstants.PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics", 1, ConfigDef.Width.MEDIUM, ReThinkConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY)

    .define(ReThinkConfigConstants.BATCH_SIZE, Type.INT, ReThinkConfigConstants.BATCH_SIZE_DEFAULT, Importance.MEDIUM,
      ReThinkConfigConstants.BATCH_SIZE_DOC, "Metrics", 2, ConfigDef.Width.MEDIUM, ReThinkConfigConstants.BATCH_SIZE)
    .define(ReThinkConfigConstants.SOURCE_LINGER_MS, Type.INT, ReThinkConfigConstants.SOURCE_LINGER_MS_DEFAULT, Importance.MEDIUM,
      ReThinkConfigConstants.SOURCE_LINGER_MS_DOC, "Metrics", 3, ConfigDef.Width.MEDIUM, ReThinkConfigConstants.SOURCE_LINGER_MS)


}
