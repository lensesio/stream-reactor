/**
  * Copyright 2016 Datamountaineer.
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

package com.datamountaineer.streamreactor.connect.kudu.config

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

/**
  * Created by andrew@datamountaineer.com on 22/02/16. 
  * stream-reactor
  */

object KuduSinkConfig {
  val KUDU_MASTER = "connect.kudu.master"
  val KUDU_MASTER_DOC = "Kudu master cluster."
  val KUDU_MASTER_DEFAULT = "localhost"
  val EXPORT_ROUTE_QUERY = "connect.kudu.export.route.query"
  val EXPORT_ROUTE_QUERY_DOC =  "KCQL expression describing field selection and routes."

  val ERROR_POLICY = "connect.kudu.sink.error.policy"
  val ERROR_POLICY_DOC = "Specifies the action to be taken if an error occurs while inserting the data.\n" +
    "There are two available options: \n" + "NOOP - the error is swallowed \n" +
    "THROW - the error is allowed to propagate. \n" +
    "RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on \n" +
    "The error will be logged automatically"
  val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL = "connect.kudu.sink.retry.interval"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"
  val NBR_OF_RETRIES = "connect.kudu.max.retires"
  val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20

  val BATCH_SIZE = "connect.kudu.sink.batch.size"
  val BATCH_SIZE_DOC = "Per topic the number of sink records to batch together and insert into Kudu"
  val BATCH_SIZE_DEFAULT = 1000

  val SCHEMA_REGISTRY_URL = "connect.kudu.sink.schema.registry.url"
  val SCHEMA_REGISTRY_URL_DOC = "Url for the schema registry"
  val SCHEMA_REGISTRY_URL_DEFAULT = "http://localhost:8081"

  val BUCKET_SIZE  = "connect.kudu.sink.bucket.size"
  val BUCKET_SIZE_DOC = "The number of buckets to use for Auto Table creation. The distribution is set to use the Hash strategy"
  val BUCKET_SIZE_DEFAULT = 10

  val config: ConfigDef = new ConfigDef()
    .define(KUDU_MASTER, Type.STRING, KUDU_MASTER_DEFAULT, Importance.HIGH, KUDU_MASTER_DOC,
      "Connection", 1, ConfigDef.Width.MEDIUM, KUDU_MASTER)
    .define(EXPORT_ROUTE_QUERY, Type.STRING, Importance.HIGH, EXPORT_ROUTE_QUERY,
      "Connection", 2, ConfigDef.Width.MEDIUM, EXPORT_ROUTE_QUERY)
    .define(ERROR_POLICY, Type.STRING, ERROR_POLICY_DEFAULT, Importance.HIGH, ERROR_POLICY_DOC,
      "Connection", 3, ConfigDef.Width.MEDIUM, ERROR_POLICY)
    .define(ERROR_RETRY_INTERVAL, Type.INT, ERROR_RETRY_INTERVAL_DEFAULT, Importance.MEDIUM, ERROR_RETRY_INTERVAL_DOC,
      "Connection", 4, ConfigDef.Width.MEDIUM, ERROR_RETRY_INTERVAL)
    .define(NBR_OF_RETRIES, Type.INT, NBR_OF_RETIRES_DEFAULT, Importance.MEDIUM, NBR_OF_RETRIES_DOC,
      "Connection", 5, ConfigDef.Width.MEDIUM, NBR_OF_RETRIES)
    .define(BATCH_SIZE, Type.INT, BATCH_SIZE_DEFAULT, Importance.MEDIUM, BATCH_SIZE_DOC,
      "Connection", 6, ConfigDef.Width.MEDIUM, BATCH_SIZE)
    .define(SCHEMA_REGISTRY_URL, Type.STRING, SCHEMA_REGISTRY_URL_DEFAULT ,Importance.HIGH, SCHEMA_REGISTRY_URL_DOC,
      "Connection", 7, ConfigDef.Width.MEDIUM, SCHEMA_REGISTRY_URL)
    .define(BUCKET_SIZE, Type.INT, BUCKET_SIZE_DEFAULT, Importance.MEDIUM, BUCKET_SIZE_DOC,
      "Connection", 8, ConfigDef.Width.MEDIUM, BUCKET_SIZE)
}

class KuduSinkConfig(props: util.Map[String, String])
  extends AbstractConfig(KuduSinkConfig.config, props)
