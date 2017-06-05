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

package com.datamountaineer.streamreactor.connect.kudu.config

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

/**
  * Created by andrew@datamountaineer.com on 22/02/16. 
  * stream-reactor
  */

object KuduSinkConfig {


  val config: ConfigDef = new ConfigDef()
    .define(KuduSinkConfigConstants.KUDU_MASTER, Type.STRING, KuduSinkConfigConstants.KUDU_MASTER_DEFAULT, Importance.HIGH, KuduSinkConfigConstants.KUDU_MASTER_DOC,
      "Connection", 1, ConfigDef.Width.MEDIUM, KuduSinkConfigConstants.KUDU_MASTER)
    .define(KuduSinkConfigConstants.EXPORT_ROUTE_QUERY, Type.STRING, Importance.HIGH, KuduSinkConfigConstants.EXPORT_ROUTE_QUERY,
      "Connection", 2, ConfigDef.Width.MEDIUM, KuduSinkConfigConstants.EXPORT_ROUTE_QUERY)
    .define(KuduSinkConfigConstants.ERROR_POLICY, Type.STRING, KuduSinkConfigConstants.ERROR_POLICY_DEFAULT, Importance.HIGH, KuduSinkConfigConstants.ERROR_POLICY_DOC,
      "Connection", 3, ConfigDef.Width.MEDIUM, KuduSinkConfigConstants.ERROR_POLICY)
    .define(KuduSinkConfigConstants.ERROR_RETRY_INTERVAL, Type.INT, KuduSinkConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT, Importance.MEDIUM, KuduSinkConfigConstants.ERROR_RETRY_INTERVAL_DOC,
      "Connection", 4, ConfigDef.Width.MEDIUM, KuduSinkConfigConstants.ERROR_RETRY_INTERVAL)
    .define(KuduSinkConfigConstants.NBR_OF_RETRIES, Type.INT, KuduSinkConfigConstants.NBR_OF_RETIRES_DEFAULT, Importance.MEDIUM, KuduSinkConfigConstants.NBR_OF_RETRIES_DOC,
      "Connection", 5, ConfigDef.Width.MEDIUM, KuduSinkConfigConstants.NBR_OF_RETRIES)
    .define(KuduSinkConfigConstants.SCHEMA_REGISTRY_URL, Type.STRING, KuduSinkConfigConstants.SCHEMA_REGISTRY_URL_DEFAULT, Importance.HIGH, KuduSinkConfigConstants.SCHEMA_REGISTRY_URL_DOC,
      "Connection", 6, ConfigDef.Width.MEDIUM, KuduSinkConfigConstants.SCHEMA_REGISTRY_URL)
}

class KuduSinkConfig(props: util.Map[String, String])
  extends AbstractConfig(KuduSinkConfig.config, props)
