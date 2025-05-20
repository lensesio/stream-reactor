/*
 * Copyright 2017-2025 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.cassandra.config

import io.lenses.streamreactor.common.config.base.traits.BaseConfig
import io.lenses.streamreactor.common.config.base.traits.ConsistencyLevelSettings
import io.lenses.streamreactor.common.config.base.traits.ErrorPolicySettings
import io.lenses.streamreactor.common.config.base.traits.KcqlSettings
import io.lenses.streamreactor.common.config.base.traits.NumberRetriesSettings
import io.lenses.streamreactor.common.config.base.traits.ThreadPoolSettings
import com.datastax.driver.core.ConsistencyLevel
import io.lenses.kcql.Field
import io.lenses.kcql.Kcql
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

import scala.collection.immutable.ListSet
import scala.jdk.CollectionConverters.ListHasAsScala

/**
  * Holds the base configuration.
  */
case class CassandraConfig() {

  val configDef: ConfigDef = new ConfigDef()
    .define(
      CassandraConfigConstants.CONTACT_POINTS,
      Type.STRING,
      CassandraConfigConstants.CONTACT_POINT_DEFAULT,
      Importance.HIGH,
      CassandraConfigConstants.CONTACT_POINT_DOC,
      "Connection",
      1,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.CONTACT_POINTS,
    )
    .define(
      CassandraConfigConstants.PORT,
      Type.INT,
      CassandraConfigConstants.PORT_DEFAULT,
      Importance.HIGH,
      CassandraConfigConstants.PORT_DOC,
      "Connection",
      2,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.PORT,
    )
    .define(
      CassandraConfigConstants.KEY_SPACE,
      Type.STRING,
      Importance.HIGH,
      CassandraConfigConstants.KEY_SPACE_DOC,
      "Connection",
      3,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.KEY_SPACE,
    )
    .define(
      CassandraConfigConstants.USERNAME,
      Type.STRING,
      "",
      Importance.HIGH,
      CassandraConfigConstants.USERNAME_DOC,
      "Connection",
      4,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.USERNAME,
    )
    .define(
      CassandraConfigConstants.PASSWD,
      Type.PASSWORD,
      "",
      Importance.LOW,
      CassandraConfigConstants.PASSWD_DOC,
      "Connection",
      5,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.PASSWD,
    )
    .define(
      CassandraConfigConstants.SSL_ENABLED,
      Type.BOOLEAN,
      CassandraConfigConstants.SSL_ENABLED_DEFAULT,
      Importance.LOW,
      CassandraConfigConstants.SSL_ENABLED_DOC,
      "Connection",
      6,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.SSL_ENABLED,
    )
    .define(
      CassandraConfigConstants.TRUST_STORE_PATH,
      Type.STRING,
      "",
      Importance.LOW,
      CassandraConfigConstants.TRUST_STORE_PATH_DOC,
      "Connection",
      7,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.TRUST_STORE_PATH,
    )
    .define(
      CassandraConfigConstants.TRUST_STORE_PASSWD,
      Type.PASSWORD,
      "",
      Importance.LOW,
      CassandraConfigConstants.TRUST_STORE_PASSWD_DOC,
      "Connection",
      8,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.TRUST_STORE_PASSWD,
    )
    .define(
      CassandraConfigConstants.TRUST_STORE_TYPE,
      Type.STRING,
      CassandraConfigConstants.TRUST_STORE_TYPE_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.TRUST_STORE_TYPE_DOC,
      "Connection",
      9,
      ConfigDef.Width.MEDIUM,
      CassandraConfigConstants.TRUST_STORE_TYPE,
    )
    .define(
      CassandraConfigConstants.KEY_STORE_TYPE,
      Type.STRING,
      CassandraConfigConstants.KEY_STORE_TYPE_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.KEY_STORE_TYPE_DOC,
      "Connection",
      10,
      ConfigDef.Width.MEDIUM,
      CassandraConfigConstants.KEY_STORE_TYPE,
    )
    .define(
      CassandraConfigConstants.USE_CLIENT_AUTH,
      Type.BOOLEAN,
      CassandraConfigConstants.USE_CLIENT_AUTH_DEFAULT,
      Importance.LOW,
      CassandraConfigConstants.USE_CLIENT_AUTH_DOC,
      "Connection",
      11,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.USE_CLIENT_AUTH,
    )
    .define(
      CassandraConfigConstants.KEY_STORE_PATH,
      Type.STRING,
      "",
      Importance.LOW,
      CassandraConfigConstants.KEY_STORE_PATH_DOC,
      "Connection",
      12,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.KEY_STORE_PATH,
    )
    .define(
      CassandraConfigConstants.KEY_STORE_PASSWD,
      Type.PASSWORD,
      "",
      Importance.LOW,
      CassandraConfigConstants.KEY_STORE_PASSWD_DOC,
      "Connection",
      13,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.KEY_STORE_PASSWD,
    )
    .define(
      CassandraConfigConstants.CONSISTENCY_LEVEL_CONFIG,
      Type.STRING,
      CassandraConfigConstants.CONSISTENCY_LEVEL_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.CONSISTENCY_LEVEL_DOC,
      "Connection",
      14,
      ConfigDef.Width.MEDIUM,
      CassandraConfigConstants.CONSISTENCY_LEVEL_DISPLAY,
    )
    .define(
      CassandraConfigConstants.ERROR_POLICY,
      Type.STRING,
      CassandraConfigConstants.ERROR_POLICY_DEFAULT,
      Importance.HIGH,
      CassandraConfigConstants.ERROR_POLICY_DOC,
      "Error",
      1,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.ERROR_POLICY,
    )
    .define(
      CassandraConfigConstants.NBR_OF_RETRIES,
      Type.INT,
      CassandraConfigConstants.NBR_OF_RETIRES_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.NBR_OF_RETRIES_DOC,
      "Error",
      2,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.NBR_OF_RETRIES,
    )
    .define(
      CassandraConfigConstants.ERROR_RETRY_INTERVAL,
      Type.INT,
      CassandraConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.ERROR_RETRY_INTERVAL_DOC,
      "Error",
      3,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.ERROR_RETRY_INTERVAL,
    )
    .define(
      CassandraConfigConstants.FETCH_SIZE,
      Type.INT,
      CassandraConfigConstants.FETCH_SIZE_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.FETCH_SIZE_DOC,
      "Connection",
      15,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.FETCH_SIZE,
    )
    .define(
      CassandraConfigConstants.LOAD_BALANCING_POLICY,
      Type.STRING,
      LoadBalancingPolicy.TOKEN_AWARE.toString,
      Importance.MEDIUM,
      CassandraConfigConstants.LOAD_BALANCING_POLICY_DOC,
      "Connection",
      16,
      ConfigDef.Width.MEDIUM,
      CassandraConfigConstants.LOAD_BALANCING_POLICY,
    )
    .define(
      CassandraConfigConstants.CONNECT_TIMEOUT,
      Type.INT,
      CassandraConfigConstants.DEFAULT_CONNECT_TIMEOUT,
      Importance.MEDIUM,
      CassandraConfigConstants.CONNECT_TIMEOUT_DOC,
      "Connection",
      14,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.CONNECT_TIMEOUT,
    )
    .define(
      CassandraConfigConstants.READ_TIMEOUT,
      Type.INT,
      CassandraConfigConstants.DEFAULT_READ_TIMEOUT,
      Importance.MEDIUM,
      CassandraConfigConstants.READ_TIMEOUT_DOC,
      "Connection",
      14,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.READ_TIMEOUT,
    )

}

/**
  * Holds the extra configurations for the source on top of
  * the base.
  */
object CassandraConfigSource {
  val base: ConfigDef = CassandraConfig().configDef
  val sourceConfig: ConfigDef = base
    .define(
      CassandraConfigConstants.ASSIGNED_TABLES,
      Type.STRING,
      "",
      Importance.LOW,
      CassandraConfigConstants.ASSIGNED_TABLES_DOC,
      "Import",
      4,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.ASSIGNED_TABLES,
    )
    .define(
      CassandraConfigConstants.KCQL,
      Type.STRING,
      Importance.HIGH,
      CassandraConfigConstants.KCQL_DOC,
      "Mappings",
      2,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.KCQL,
    )
    .define(
      CassandraConfigConstants.READER_BUFFER_SIZE,
      Type.INT,
      CassandraConfigConstants.READER_BUFFER_SIZE_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.READER_BUFFER_SIZE_DOC,
      "Import",
      3,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.READER_BUFFER_SIZE,
    )
    .define(
      CassandraConfigConstants.BATCH_SIZE,
      Type.INT,
      CassandraConfigConstants.BATCH_SIZE_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.BATCH_SIZE_DOC,
      "Import",
      5,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.BATCH_SIZE,
    )
    .define(
      CassandraConfigConstants.POLL_INTERVAL,
      Type.LONG,
      CassandraConfigConstants.DEFAULT_POLL_INTERVAL,
      Importance.MEDIUM,
      CassandraConfigConstants.POLL_INTERVAL_DOC,
      "Import",
      6,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.POLL_INTERVAL,
    )
    .define(
      CassandraConfigConstants.ALLOW_FILTERING,
      Type.BOOLEAN,
      CassandraConfigConstants.ALLOW_FILTERING_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.ALLOW_FILTERING_DOC,
      "Import",
      7,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.ALLOW_FILTERING,
    )
    .define(
      CassandraConfigConstants.TIME_SLICE_MILLIS,
      Type.LONG,
      CassandraConfigConstants.TIME_SLICE_MILLIS_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.TIME_SLICE_MILLIS_DOC,
      "Import",
      7,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.TIME_SLICE_MILLIS,
    )
    .define(
      CassandraConfigConstants.TIMESLICE_DURATION,
      Type.LONG,
      CassandraConfigConstants.TIMESLICE_DURATION_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.TIMESLICE_DURATION_DOC,
      "Import",
      8,
      ConfigDef.Width.SHORT,
      CassandraConfigConstants.TIMESLICE_DURATION,
    )
    .define(
      CassandraConfigConstants.TIMESLICE_DELAY,
      Type.LONG,
      CassandraConfigConstants.TIMESLICE_DELAY_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.TIMESLICE_DELAY_DOC,
      "Import",
      9,
      ConfigDef.Width.SHORT,
      CassandraConfigConstants.TIMESLICE_DELAY,
    )
    .define(
      CassandraConfigConstants.INITIAL_OFFSET,
      Type.STRING,
      CassandraConfigConstants.INITIAL_OFFSET_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.INITIAL_OFFSET_DOC,
      "Import",
      10,
      ConfigDef.Width.MEDIUM,
      CassandraConfigConstants.INITIAL_OFFSET,
    )
    .define(
      CassandraConfigConstants.MAPPING_COLLECTION_TO_JSON,
      Type.BOOLEAN,
      CassandraConfigConstants.MAPPING_COLLECTION_TO_JSON_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.MAPPING_COLLECTION_TO_JSON_DOC,
      "Import",
      11,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.MAPPING_COLLECTION_TO_JSON,
    )
    .define(
      CassandraConfigConstants.BUCKET_TIME_SERIES_MODE,
      Type.STRING,
      "",
      Importance.MEDIUM,
      CassandraConfigConstants.BUCKET_TIME_SERIES_MODE_DOC,
      "Import",
      12,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.BUCKET_TIME_SERIES_MODE,
    )
    .define(
      CassandraConfigConstants.BUCKET_TIME_SERIES_FORMAT,
      Type.STRING,
      "",
      Importance.MEDIUM,
      CassandraConfigConstants.BUCKET_TIME_SERIES_FORMAT_DOC,
      "Import",
      13,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.BUCKET_TIME_SERIES_FORMAT,
    )
    .define(
      CassandraConfigConstants.BUCKET_TIME_SERIES_FIELD_NAME,
      Type.STRING,
      CassandraConfigConstants.BUCKET_TIME_SERIES_FIELD_NAME_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.BUCKET_TIME_SERIES_FIELD_NAME_DOC,
      "Import",
      13,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.BUCKET_TIME_SERIES_FIELD_NAME,
    )

}

case class CassandraConfigSource(props: Map[String, String])
    extends BaseConfig(CassandraConfigConstants.CONNECTOR_PREFIX, CassandraConfigSource.sourceConfig, props)
    with ErrorPolicySettings
    with ConsistencyLevelSettings[ConsistencyLevel]
    with KcqlSettings
    with CassandraFieldsSettings

/**
  * Holds the extra configurations for the sink on top of
  * the base.
  */
object CassandraConfigSink {
  val base: ConfigDef = CassandraConfig().configDef
  val sinkConfig = base
    .define(
      CassandraConfigConstants.KCQL,
      Type.STRING,
      Importance.HIGH,
      CassandraConfigConstants.KCQL_DOC,
      "Mappings",
      1,
      ConfigDef.Width.LONG,
      CassandraConfigConstants.KCQL,
    )
    .define(
      CassandraConfigConstants.THREAD_POOL_CONFIG,
      Type.INT,
      CassandraConfigConstants.THREAD_POOL_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.THREAD_POOL_DOC,
      "Import",
      8,
      ConfigDef.Width.MEDIUM,
      CassandraConfigConstants.THREAD_POOL_DISPLAY,
    )
    .define(
      CassandraConfigConstants.PROGRESS_COUNTER_ENABLED,
      Type.BOOLEAN,
      CassandraConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
      Importance.MEDIUM,
      CassandraConfigConstants.PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics",
      1,
      ConfigDef.Width.MEDIUM,
      CassandraConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY,
    )
    .define(
      CassandraConfigConstants.DELETE_ROW_ENABLED,
      Type.BOOLEAN,
      CassandraConfigConstants.DELETE_ROW_ENABLED_DEFAULT,
      Importance.LOW,
      CassandraConfigConstants.DELETE_ROW_ENABLED_DOC,
      "Mappings",
      1,
      ConfigDef.Width.MEDIUM,
      CassandraConfigConstants.DELETE_ROW_ENABLED_DISPLAY,
    )
    .define(
      CassandraConfigConstants.DELETE_ROW_STATEMENT,
      Type.STRING,
      CassandraConfigConstants.DELETE_ROW_STATEMENT_DEFAULT,
      Importance.LOW,
      CassandraConfigConstants.DELETE_ROW_STATEMENT_DOC,
      "Mappings",
      1,
      ConfigDef.Width.MEDIUM,
      CassandraConfigConstants.DELETE_ROW_STATEMENT_DISPLAY,
    )
    .define(
      CassandraConfigConstants.DELETE_ROW_STRUCT_FLDS,
      Type.LIST,
      CassandraConfigConstants.DELETE_ROW_STRUCT_FLDS_DEFAULT,
      Importance.LOW,
      CassandraConfigConstants.DELETE_ROW_STRUCT_FLDS_DOC,
      "Mappings",
      1,
      ConfigDef.Width.MEDIUM,
      CassandraConfigConstants.DELETE_ROW_STRUCT_FLDS_DISPLAY,
    )
    .define(
      CassandraConfigConstants.DEFAULT_VALUE_SERVE_STRATEGY_PROPERTY,
      Type.STRING,
      CassandraConfigConstants.DEFAULT_VALUE_SERVE_STRATEGY_DEFAULT,
      Importance.LOW,
      CassandraConfigConstants.DEFAULT_VALUE_SERVE_STRATEGY_DOC,
      "Mappings",
      1,
      ConfigDef.Width.MEDIUM,
      CassandraConfigConstants.DEFAULT_VALUE_SERVE_STRATEGY_DISPLAY,
    )

}

trait CassandraFieldsSettings {

  def getKCQL: Set[Kcql]

  def getPrimaryKeyCols(kcql: Set[Kcql] = getKCQL): Map[String, Set[String]] =
    kcql.toList
      .map(k => (k.getSource, ListSet(k.getPrimaryKeys.asScala.map(p => p.getName).reverse.toSeq: _*).toSet))
      .toMap

  def getFields(kcql: Set[Kcql] = getKCQL): Map[String, Seq[Field]] =
    kcql.toList.map(rm => (rm.getSource, rm.getFields.asScala.toSeq)).toMap

  def getIgnoreFields(kcql: Set[Kcql] = getKCQL): Map[String, Seq[Field]] =
    kcql.toList.map(rm => (rm.getSource, rm.getIgnoredFields.asScala.toSeq)).toMap

  def getIncrementalMode(routes: Set[Kcql]): Map[String, String] =
    routes.toList.map(r => (r.getSource, r.getIncrementalMode)).toMap

}

case class CassandraConfigSink(props: Map[String, String])
    extends BaseConfig(CassandraConfigConstants.CONNECTOR_PREFIX, CassandraConfigSink.sinkConfig, props)
    with KcqlSettings
    with ErrorPolicySettings
    with NumberRetriesSettings
    with ThreadPoolSettings
    with ConsistencyLevelSettings[ConsistencyLevel]
    with CassandraFieldsSettings {}
