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

package com.datamountaineer.streamreactor.connect.cassandra.config

import java.util

import com.datamountaineer.streamreactor.temp.{ErrorPolicySettings, NumberRetriesSettings, KcqlSettings,
                                               ThreadPoolSettings, ConsistencyLevelSettings}
import com.datastax.driver.core.ConsistencyLevel
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

/**
  * Holds the base configuration.
  **/
case class CassandraConfig() {

  val configDef: ConfigDef = new ConfigDef()
    .define(CassandraConfigConstants.CONTACT_POINTS,
      Type.STRING,
      CassandraConfigConstants.CONTACT_POINT_DEFAULT,
      Importance.HIGH,
      CassandraConfigConstants.CONTACT_POINT_DOC,
      "Connection",
      1,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.CONTACT_POINTS
    )

    .define(CassandraConfigConstants.PORT,
      Type.INT,
      CassandraConfigConstants.PORT_DEFAULT,
      Importance.HIGH,
      CassandraConfigConstants.PORT_DOC,
      "Connection",
      2,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.PORT)

    .define(CassandraConfigConstants.KEY_SPACE,
      Type.STRING,
      Importance.HIGH,
      CassandraConfigConstants.KEY_SPACE_DOC,
      "Connection",
      3,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.KEY_SPACE)

    .define(CassandraConfigConstants.USERNAME,
      Type.STRING, "",
      Importance.HIGH,
      CassandraConfigConstants.USERNAME_DOC,
      "Connection",
      4,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.USERNAME)

    .define(CassandraConfigConstants.PASSWD,
      Type.PASSWORD, "",
      Importance.LOW,
      CassandraConfigConstants.PASSWD_DOC,
      "Connection",
      5,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.PASSWD
    )

    .define(CassandraConfigConstants.SSL_ENABLED,
      Type.BOOLEAN,
      CassandraConfigConstants.SSL_ENABLED_DEFAULT,
      Importance.LOW,
      CassandraConfigConstants.SSL_ENABLED_DOC,
      "Connection",
      6,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.SSL_ENABLED)

    .define(CassandraConfigConstants.TRUST_STORE_PATH,
      Type.STRING,
      "",
      Importance.LOW,
      CassandraConfigConstants.TRUST_STORE_PATH_DOC,
      "Connection",
      7,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.TRUST_STORE_PATH)

    .define(CassandraConfigConstants.TRUST_STORE_PASSWD,
      Type.PASSWORD,
      "",
      Importance.LOW,
      CassandraConfigConstants.TRUST_STORE_PASSWD_DOC,
      "Connection",
      8,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.TRUST_STORE_PASSWD)

    .define(CassandraConfigConstants.TRUST_STORE_TYPE,
      Type.STRING,
      CassandraConfigConstants.TRUST_STORE_TYPE_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.TRUST_STORE_TYPE_DOC,
      "Connection",
      9,
      ConfigDef.Width.MEDIUM,
      CassandraConfigConstants.TRUST_STORE_TYPE)

    .define(CassandraConfigConstants.KEY_STORE_TYPE,
      Type.STRING,
      CassandraConfigConstants.KEY_STORE_TYPE_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.KEY_STORE_TYPE_DOC,
      "Connection",
      10,
      ConfigDef.Width.MEDIUM,
      CassandraConfigConstants.KEY_STORE_TYPE)

    .define(CassandraConfigConstants.USE_CLIENT_AUTH,
      Type.BOOLEAN,
      CassandraConfigConstants.USE_CLIENT_AUTH_DEFAULT,
      Importance.LOW,
      CassandraConfigConstants.USE_CLIENT_AUTH_DOC,
      "Connection",
      11,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.USE_CLIENT_AUTH)

    .define(CassandraConfigConstants.KEY_STORE_PATH,
      Type.STRING,
      "",
      Importance.LOW,
      CassandraConfigConstants.KEY_STORE_PATH_DOC,
      "Connection",
      12,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.KEY_STORE_PATH)

    .define(CassandraConfigConstants.KEY_STORE_PASSWD,
      Type.PASSWORD,
      "",
      Importance.LOW,
      CassandraConfigConstants.KEY_STORE_PASSWD_DOC,
      "Connection",
      13,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.KEY_STORE_PASSWD)

    .define(CassandraConfigConstants.CONSISTENCY_LEVEL_CONFIG,
      Type.STRING,
      CassandraConfigConstants.CONSISTENCY_LEVEL_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.CONSISTENCY_LEVEL_DOC,
      "Connection",
      14,
      ConfigDef.Width.MEDIUM,
      CassandraConfigConstants.CONSISTENCY_LEVEL_DISPLAY)

    .define(CassandraConfigConstants.ERROR_POLICY,
      Type.STRING,
      CassandraConfigConstants.ERROR_POLICY_DEFAULT,
      Importance.HIGH,
      CassandraConfigConstants.ERROR_POLICY_DOC,
      "Error",
      1,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.ERROR_POLICY)

    .define(CassandraConfigConstants.NBR_OF_RETRIES,
      Type.INT,
      CassandraConfigConstants.NBR_OF_RETIRES_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.NBR_OF_RETRIES_DOC,
      "Error",
      2,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.NBR_OF_RETRIES)

    .define(CassandraConfigConstants.ERROR_RETRY_INTERVAL,
      Type.INT,
      CassandraConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.ERROR_RETRY_INTERVAL_DOC,
      "Error",
      3,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.ERROR_RETRY_INTERVAL)

}


/**
  * Holds the extra configurations for the source on top of
  * the base.
  **/
object CassandraConfigSource {
  val base: ConfigDef = CassandraConfig().configDef
  val sourceConfig: ConfigDef = base
    .define(CassandraConfigConstants.IMPORT_MODE,
      Type.STRING,
      Importance.HIGH,
      CassandraConfigConstants.IMPORT_MODE_DOC,
      "Import",
      1,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.IMPORT_MODE)

    .define(CassandraConfigConstants.ASSIGNED_TABLES,
      Type.STRING,
      "",
      Importance.LOW,
      CassandraConfigConstants.ASSIGNED_TABLES_DOC,
      "Import",
      4,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.ASSIGNED_TABLES)

    .define(CassandraConfigConstants.SOURCE_KCQL_QUERY,
      Type.STRING,
      Importance.HIGH,
      CassandraConfigConstants.SOURCE_KCQL_DOC,
      "Mappings",
      2,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.SOURCE_KCQL_QUERY)


    .define(CassandraConfigConstants.READER_BUFFER_SIZE,
      Type.INT,
      CassandraConfigConstants.READER_BUFFER_SIZE_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.READER_BUFFER_SIZE_DOC,
      "Import",
      3,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.READER_BUFFER_SIZE)


    .define(CassandraConfigConstants.BATCH_SIZE,
      Type.INT,
      CassandraConfigConstants.BATCH_SIZE_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.BATCH_SIZE_DOC,
      "Import",
      5,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.BATCH_SIZE)

    .define(CassandraConfigConstants.POLL_INTERVAL,
      Type.LONG,
      CassandraConfigConstants.DEFAULT_POLL_INTERVAL,
      Importance.MEDIUM,
      CassandraConfigConstants.POLL_INTERVAL_DOC,
      "Import",
      6,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.POLL_INTERVAL)


    .define(CassandraConfigConstants.ALLOW_FILTERING,
      Type.BOOLEAN,
      CassandraConfigConstants.ALLOW_FILTERING_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.ALLOW_FILTERING_DOC,
      "Import",
      7,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.ALLOW_FILTERING)

    .define(CassandraConfigConstants.TIMESTAMP_TYPE,
      Type.STRING,
      CassandraConfigConstants.TIMESTAMP_TYPE_DEFAULT,
      Importance.HIGH,
      CassandraConfigConstants.TIMESTAMP_TYPE_DOC,
      "Import",
      8,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.TIMESTAMP_TYPE)

}

case class CassandraConfigSource(props: util.Map[String, String])
  extends AbstractConfig(CassandraConfigSource.sourceConfig, props)
    with ErrorPolicySettings
    with KcqlSettings
    with ConsistencyLevelSettings[ConsistencyLevel] {
  override val errorPolicyConstant: String = CassandraConfigConstants.ERROR_POLICY
  override val kcqlConstant: String = CassandraConfigConstants.SOURCE_KCQL_QUERY
  override val consistencyLevelConstant: String = CassandraConfigConstants.CONSISTENCY_LEVEL_CONFIG
}

/**
  * Holds the extra configurations for the sink on top of
  * the base.
  **/
object CassandraConfigSink {
  val base = CassandraConfig().configDef
  val sinkConfig = base
    .define(CassandraConfigConstants.SINK_KCQL,
      Type.STRING,
      Importance.HIGH,
      CassandraConfigConstants.SINK_KCQL_DOC,
      "Mappings",
      1,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.SINK_KCQL)
    .define(CassandraConfigConstants.SINK_THREAD_POOL_CONFIG,
      Type.INT,
      CassandraConfigConstants.SINK_THREAD_POOL_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.SINK_THREAD_POOL_DOC,
      "Import",
      8,
      ConfigDef.Width.MEDIUM,
      CassandraConfigConstants.SINK_THREAD_POOL_DISPLAY
    )
}

case class CassandraConfigSink(props: util.Map[String, String])
  extends AbstractConfig(CassandraConfigSink.sinkConfig, props)
    with ErrorPolicySettings
    with NumberRetriesSettings
    with KcqlSettings
    with ThreadPoolSettings
    with ConsistencyLevelSettings[ConsistencyLevel] {
  override val errorPolicyConstant: String = CassandraConfigConstants.ERROR_POLICY
  override val kcqlConstant: String = CassandraConfigConstants.SINK_KCQL
  override val numberRetriesConstant: String = CassandraConfigConstants.NBR_OF_RETRIES
  override val threadPoolConstant: String = CassandraConfigConstants.SINK_THREAD_POOL_CONFIG
  override val consistencyLevelConstant: String = CassandraConfigConstants.CONSISTENCY_LEVEL_CONFIG
}
