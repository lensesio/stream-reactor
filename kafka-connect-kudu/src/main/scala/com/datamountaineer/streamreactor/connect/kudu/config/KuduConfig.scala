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

import com.datamountaineer.streamreactor.connect.config.base.traits._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

/**
  * Created by andrew@datamountaineer.com on 22/02/16. 
  * stream-reactor
  */

object KuduConfig {


  val config: ConfigDef = new ConfigDef()
    .define(KuduConfigConstants.KUDU_MASTER, Type.STRING, KuduConfigConstants.KUDU_MASTER_DEFAULT,
      Importance.HIGH, KuduConfigConstants.KUDU_MASTER_DOC,
      "Connection", 1, ConfigDef.Width.MEDIUM, KuduConfigConstants.KUDU_MASTER)

    .define(KuduConfigConstants.KCQL, Type.STRING, Importance.HIGH, KuduConfigConstants.KCQL,
      "Connection", 2, ConfigDef.Width.MEDIUM, KuduConfigConstants.KCQL)

    .define(KuduConfigConstants.ERROR_POLICY, Type.STRING, KuduConfigConstants.ERROR_POLICY_DEFAULT,
      Importance.HIGH, KuduConfigConstants.ERROR_POLICY_DOC,
      "Connection", 3, ConfigDef.Width.MEDIUM, KuduConfigConstants.ERROR_POLICY)

    .define(KuduConfigConstants.ERROR_RETRY_INTERVAL, Type.INT, KuduConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT,
      Importance.MEDIUM, KuduConfigConstants.ERROR_RETRY_INTERVAL_DOC,
      "Connection", 4, ConfigDef.Width.MEDIUM, KuduConfigConstants.ERROR_RETRY_INTERVAL)

    .define(KuduConfigConstants.NBR_OF_RETRIES, Type.INT, KuduConfigConstants.NBR_OF_RETIRES_DEFAULT,
      Importance.MEDIUM, KuduConfigConstants.NBR_OF_RETRIES_DOC,
      "Connection", 5, ConfigDef.Width.MEDIUM, KuduConfigConstants.NBR_OF_RETRIES)

    .define(KuduConfigConstants.SCHEMA_REGISTRY_URL, Type.STRING,
      KuduConfigConstants.SCHEMA_REGISTRY_URL_DEFAULT, Importance.HIGH, KuduConfigConstants.SCHEMA_REGISTRY_URL_DOC,
      "Connection", 6, ConfigDef.Width.MEDIUM,
      KuduConfigConstants.SCHEMA_REGISTRY_URL)

    .define(KuduConfigConstants.WRITE_FLUSH_MODE, Type.STRING, KuduConfigConstants.WRITE_FLUSH_MODE_DEFAULT,
        Importance.MEDIUM, KuduConfigConstants.WRITE_FLUSH_MODE_DOC,
        "Connection", 7, ConfigDef.Width.MEDIUM,
        KuduConfigConstants.WRITE_FLUSH_MODE)

    .define(KuduConfigConstants.PROGRESS_COUNTER_ENABLED, Type.BOOLEAN, KuduConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
      Importance.MEDIUM, KuduConfigConstants.PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics", 1, ConfigDef.Width.MEDIUM, KuduConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY)


}

class KuduConfig(props: util.Map[String, String])
  extends BaseConfig(KuduConfigConstants.CONNECTOR_PREFIX, KuduConfig.config, props)
    with KcqlSettings
    with DatabaseSettings
    with ErrorPolicySettings
    with NumberRetriesSettings
    with ConnectionSettings {

  def getWriteFlushMode() = WriteFlushMode.withName(
    props.getOrDefault(
      KuduConfigConstants.WRITE_FLUSH_MODE,
      KuduConfigConstants.WRITE_FLUSH_MODE_DEFAULT).toUpperCase)

}


