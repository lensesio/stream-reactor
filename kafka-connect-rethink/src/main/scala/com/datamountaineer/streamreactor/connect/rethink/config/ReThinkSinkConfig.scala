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

import java.util

import com.datamountaineer.streamreactor.temp._
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

/**
  * Created by andrew@datamountaineer.com on 24/03/16. 
  * stream-reactor
  */
object ReThinkSinkConfig {
  val config: ConfigDef = new ConfigDef()
    .define(ReThinkSinkConfigConstants.RETHINK_HOST, Type.STRING,
      ReThinkSinkConfigConstants.RETHINK_HOST_DEFAULT,
      Importance.HIGH, ReThinkSinkConfigConstants.RETHINK_HOST_DOC,
      "Connection", 1, ConfigDef.Width.MEDIUM, ReThinkSinkConfigConstants.RETHINK_HOST)
    .define(ReThinkSinkConfigConstants.RETHINK_DB, Type.STRING,
      ReThinkSinkConfigConstants.RETHINK_DB_DEFAULT,
      Importance.HIGH, ReThinkSinkConfigConstants.RETHINK_DB_DOC,
      "Connection", 2, ConfigDef.Width.MEDIUM, ReThinkSinkConfigConstants.RETHINK_DB)
    .define(ReThinkSinkConfigConstants.RETHINK_PORT, Type.INT,
      ReThinkSinkConfigConstants.RETHINK_PORT_DEFAULT,
      Importance.MEDIUM, ReThinkSinkConfigConstants.RETHINK_PORT_DOC,
      "Connection", 3, ConfigDef.Width.MEDIUM, ReThinkSinkConfigConstants.RETHINK_PORT)
    .define(ReThinkSinkConfigConstants.EXPORT_ROUTE_QUERY, Type.STRING, Importance.HIGH,
      ReThinkSinkConfigConstants.EXPORT_ROUTE_QUERY_DOC,
      "Connection", 4, ConfigDef.Width.MEDIUM, ReThinkSinkConfigConstants.EXPORT_ROUTE_QUERY)
    .define(ReThinkSinkConfigConstants.ERROR_POLICY, Type.STRING,
      ReThinkSinkConfigConstants.ERROR_POLICY_DEFAULT,
      Importance.HIGH, ReThinkSinkConfigConstants.ERROR_POLICY_DOC,
      "Connection", 5, ConfigDef.Width.MEDIUM, ReThinkSinkConfigConstants.ERROR_POLICY)
    .define(ReThinkSinkConfigConstants.ERROR_RETRY_INTERVAL, Type.INT,
      ReThinkSinkConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT,
      Importance.MEDIUM, ReThinkSinkConfigConstants.ERROR_RETRY_INTERVAL_DOC,
      "Connection", 6, ConfigDef.Width.MEDIUM, ReThinkSinkConfigConstants.ERROR_RETRY_INTERVAL)
    .define(ReThinkSinkConfigConstants.NBR_OF_RETRIES, Type.INT,
      ReThinkSinkConfigConstants.NBR_OF_RETIRES_DEFAULT,
      Importance.MEDIUM, ReThinkSinkConfigConstants.NBR_OF_RETRIES_DOC,
      "Connection", 7, ConfigDef.Width.MEDIUM, ReThinkSinkConfigConstants.NBR_OF_RETRIES)
    .define(ReThinkSinkConfigConstants.BATCH_SIZE, Type.INT,
      ReThinkSinkConfigConstants.BATCH_SIZE_DEFAULT, Importance.MEDIUM,
      ReThinkSinkConfigConstants.BATCH_SIZE_DOC,
      "Connection", 8, ConfigDef.Width.MEDIUM, ReThinkSinkConfigConstants.BATCH_SIZE)
}

case class ReThinkSinkConfig(props: util.Map[String, String])
  extends AbstractConfig(ReThinkSinkConfig.config, props)
    with ErrorPolicySettings
    with NumberRetriesSettings
    with KcqlSettings
    with BatchSizeSettings
    with DatabaseSettings
    with RetryIntervalSettings {
  override val errorPolicyConstant: String = ReThinkSinkConfigConstants.ERROR_POLICY
  override val kcqlConstant: String = ReThinkSinkConfigConstants.EXPORT_ROUTE_QUERY
  override val numberRetriesConstant: String = ReThinkSinkConfigConstants.NBR_OF_RETRIES
  override val batchSizeConstant: String = ReThinkSinkConfigConstants.BATCH_SIZE
  override val databaseConstant: String = ReThinkSinkConfigConstants.RETHINK_DB
  override val retryIntervalConstant: String = ReThinkSinkConfigConstants.ERROR_RETRY_INTERVAL
}
