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

package com.datamountaineer.streamreactor.connect.hazelcast.config

import java.util

import com.datamountaineer.streamreactor.temp.traits._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

/**
  * Created by andrew@datamountaineer.com on 08/08/16. 
  * stream-reactor
  */
object HazelCastSinkConfig {

  val config: ConfigDef = new ConfigDef()
    .define(HazelCastSinkConfigConstants.CLUSTER_MEMBERS, Type.LIST, Importance.HIGH, HazelCastSinkConfigConstants.CLUSTER_MEMBERS_DOC,
      "Connection", 1, ConfigDef.Width.MEDIUM, HazelCastSinkConfigConstants.CLUSTER_MEMBERS)
    .define(HazelCastSinkConfigConstants.CONNECTION_TIMEOUT, Type.LONG, HazelCastSinkConfigConstants.CONNECTION_TIMEOUT_DEFAULT, Importance.LOW, HazelCastSinkConfigConstants.CONNECTION_TIMEOUT_DOC,
      "Connection", 2, ConfigDef.Width.MEDIUM, HazelCastSinkConfigConstants.CONNECTION_TIMEOUT)
    .define(HazelCastSinkConfigConstants.CONNECTION_RETRY_ATTEMPTS, Type.INT, HazelCastSinkConfigConstants.CONNECTION_RETRY_ATTEMPTS_DEFAULT, Importance.LOW,
      HazelCastSinkConfigConstants.CONNECTION_RETRY_ATTEMPTS_DOC, "Connection", 3, ConfigDef.Width.MEDIUM, HazelCastSinkConfigConstants.CONNECTION_RETRY_ATTEMPTS)
    .define(HazelCastSinkConfigConstants.KEEP_ALIVE, Type.BOOLEAN, HazelCastSinkConfigConstants.KEEP_ALIVE_DEFAULT, Importance.LOW, HazelCastSinkConfigConstants.KEEP_ALIVE_DOC,
      "Connection", 3, ConfigDef.Width.MEDIUM, HazelCastSinkConfigConstants.KEEP_ALIVE)
    .define(HazelCastSinkConfigConstants.TCP_NO_DELAY, Type.BOOLEAN, HazelCastSinkConfigConstants.TCP_NO_DELAY_DEFAULT, Importance.LOW, HazelCastSinkConfigConstants.TCP_NO_DELAY_DOC,
      "Connection", 4, ConfigDef.Width.MEDIUM, HazelCastSinkConfigConstants.TCP_NO_DELAY)
    .define(HazelCastSinkConfigConstants.REUSE_ADDRESS, Type.BOOLEAN, HazelCastSinkConfigConstants.REUSE_ADDRESS_DEFAULT, Importance.LOW, HazelCastSinkConfigConstants.REUSE_ADDRESS_DOC,
      "Connection", 5, ConfigDef.Width.MEDIUM, HazelCastSinkConfigConstants.REUSE_ADDRESS)
    .define(HazelCastSinkConfigConstants.LINGER_SECONDS, Type.INT, HazelCastSinkConfigConstants.LINGER_SECONDS_DEFAULT, Importance.LOW, HazelCastSinkConfigConstants.LINGER_SECONDS_DOC,
      "Connection", 6, ConfigDef.Width.MEDIUM, HazelCastSinkConfigConstants.LINGER_SECONDS)
    .define(HazelCastSinkConfigConstants.BUFFER_SIZE, Type.INT, HazelCastSinkConfigConstants.BUFFER_SIZE_DEFAULT, Importance.LOW, HazelCastSinkConfigConstants.BUFFER_SIZE_DOC,
      "Connection", 7, ConfigDef.Width.MEDIUM, HazelCastSinkConfigConstants.BUFFER_SIZE)
    .define(HazelCastSinkConfigConstants.GROUP_NAME, Type.STRING, Importance.HIGH, HazelCastSinkConfigConstants.SINK_GROUP_NAME_DOC,
      "Connection", 8, ConfigDef.Width.MEDIUM, HazelCastSinkConfigConstants.GROUP_NAME)
    .define(HazelCastSinkConfigConstants.GROUP_PASSWORD, Type.PASSWORD, HazelCastSinkConfigConstants.SINK_GROUP_PASSWORD_DEFAULT, Importance.MEDIUM, HazelCastSinkConfigConstants.SINK_GROUP_PASSWORD_DOC,
      "Connection", 9, ConfigDef.Width.MEDIUM, HazelCastSinkConfigConstants.GROUP_PASSWORD)
    .define(HazelCastSinkConfigConstants.EXPORT_ROUTE_QUERY, Type.STRING, Importance.HIGH, HazelCastSinkConfigConstants.EXPORT_ROUTE_QUERY,
      "Target", 1, ConfigDef.Width.MEDIUM, HazelCastSinkConfigConstants.EXPORT_ROUTE_QUERY)
    .define(HazelCastSinkConfigConstants.ERROR_POLICY, Type.STRING, HazelCastSinkConfigConstants.ERROR_POLICY_DEFAULT, Importance.HIGH, HazelCastSinkConfigConstants.ERROR_POLICY_DOC,
      "Target", 2, ConfigDef.Width.MEDIUM, HazelCastSinkConfigConstants.ERROR_POLICY)
    .define(HazelCastSinkConfigConstants.ERROR_RETRY_INTERVAL, Type.INT, HazelCastSinkConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT, Importance.MEDIUM, HazelCastSinkConfigConstants.ERROR_RETRY_INTERVAL_DOC,
      "Target", 3, ConfigDef.Width.MEDIUM, HazelCastSinkConfigConstants.ERROR_RETRY_INTERVAL)
    .define(HazelCastSinkConfigConstants.NBR_OF_RETRIES, Type.INT, HazelCastSinkConfigConstants.NBR_OF_RETIRES_DEFAULT, Importance.MEDIUM, HazelCastSinkConfigConstants.NBR_OF_RETRIES_DOC,
      "Target", 4, ConfigDef.Width.MEDIUM, HazelCastSinkConfigConstants.NBR_OF_RETRIES)
    .define(HazelCastSinkConfigConstants.THREAD_POOL_CONFIG, Type.INT, HazelCastSinkConfigConstants.SINK_THREAD_POOL_DEFAULT, Importance.MEDIUM, HazelCastSinkConfigConstants.SINK_THREAD_POOL_DOC,
      "Target", 5, ConfigDef.Width.MEDIUM, HazelCastSinkConfigConstants.SINK_THREAD_POOL_DISPLAY)
    .define(HazelCastSinkConfigConstants.PARALLEL_WRITE, Type.BOOLEAN, HazelCastSinkConfigConstants.PARALLEL_WRITE_DEFAULT, Importance.MEDIUM, HazelCastSinkConfigConstants.PARALLEL_WRITE_DOC,
      "Target", 5, ConfigDef.Width.MEDIUM, HazelCastSinkConfigConstants.PARALLEL_WRITE)
}

class HazelCastSinkConfig(props: util.Map[String, String])
  extends BaseConfig(HazelCastSinkConfigConstants.HAZELCAST_CONNECTOR_PREFIX, HazelCastSinkConfig.config, props)
    with ErrorPolicySettings
    with KcqlSettings
    with NumberRetriesSettings
    with ThreadPoolSettings
    with AllowParallelizationSettings
