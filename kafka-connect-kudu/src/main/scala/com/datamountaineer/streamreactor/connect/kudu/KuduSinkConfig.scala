/**
  * Copyright 2015 Datamountaineer.
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
  **/

package com.datamountaineer.streamreactor.connect.kudu

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

/**
  * Created by andrew@datamountaineer.com on 22/02/16. 
  * stream-reactor
  */

object KuduSinkConfig {
  val KUDU_MASTER = "kudu.master"
  val KUDU_MASTER_DOC = "Kudu master cluster."
  val KUDU_MASTER_DEFAULT = "localhost"

  val config: ConfigDef = new ConfigDef()
    .define(KUDU_MASTER, Type.STRING, KUDU_MASTER_DEFAULT, Importance.HIGH, KUDU_MASTER_DOC)
}

class KuduSinkConfig(props: util.Map[String, String])
  extends AbstractConfig(KuduSinkConfig.config, props)
