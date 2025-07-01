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
package io.lenses.streamreactor.connect.azure.cosmosdb.config

import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.CONSISTENCY_LEVEL_PROP_SUFFIX
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.DATABASE_PROP_SUFFIX
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.ERROR_POLICY_PROP_SUFFIX
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.KCQL_PROP_SUFFIX
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.MAX_RETRIES_PROP_SUFFIX
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.PROGRESS_ENABLED_CONST
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.RETRY_INTERVAL_PROP_SUFFIX

/**
 * Holds the constants used in the config.
 */

object CosmosDbConfigConstants {
  val CONNECTOR_PREFIX = "connect.cosmosdb"

  val DATABASE_CONFIG     = s"$CONNECTOR_PREFIX.$DATABASE_PROP_SUFFIX"
  val DATABASE_CONFIG_DOC = "The Azure CosmosDb target database."

  val CONNECTION_CONFIG     = s"$CONNECTOR_PREFIX.endpoint"
  val CONNECTION_CONFIG_DOC = "The Azure CosmosDB end point."
  val CONNECTION_DISPLAY    = "Connection endpoint."

  val MASTER_KEY_CONFIG = s"$CONNECTOR_PREFIX.master.key"
  val MASTER_KEY_DOC    = "The connection master key"

  val ERROR_POLICY_CONFIG = s"$CONNECTOR_PREFIX.$ERROR_POLICY_PROP_SUFFIX"
  val ERROR_POLICY_DOC: String =
    """Specifies the action to be taken if an error occurs while inserting the data
      |There are two available options:
      |NOOP - the error is swallowed
      |THROW - the error is allowed to propagate.
      |RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on
      |The error will be logged automatically""".stripMargin
  val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL_CONFIG  = s"$CONNECTOR_PREFIX.$RETRY_INTERVAL_PROP_SUFFIX"
  val ERROR_RETRY_INTERVAL_DOC     = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"

  val NBR_OF_RETRIES_CONFIG  = s"$CONNECTOR_PREFIX.$MAX_RETRIES_PROP_SUFFIX"
  val NBR_OF_RETRIES_DOC     = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20

  val KCQL_CONFIG = s"$CONNECTOR_PREFIX.$KCQL_PROP_SUFFIX"

  val CONSISTENCY_CONFIG = s"$CONNECTOR_PREFIX.$CONSISTENCY_LEVEL_PROP_SUFFIX"
  val CONSISTENCY_DOC =
    "Determines the write visibility. There are four possible values: Strong,BoundedStaleness,Session or Eventual"
  val CONSISTENCY_DISPLAY = "Writes consistency"
  val CONSISTENCY_DEFAULT = "Session"

  val KEY_SOURCE_CONFIG  = s"$CONNECTOR_PREFIX.key.source"
  val KEY_SOURCE_DOC     = "The source of the key.  There are 4 possible values: Key, Metadata, KeyPath or ValuePath"
  val KEY_SOURCE_DISPLAY = "Key strategy"
  val KEY_SOURCE_DEFAULT = "Key"

  val BULK_CONFIG  = s"$CONNECTOR_PREFIX.bulk.enabled"
  val BULK_DOC     = "Enable bulk mode to reduce chatter"
  val BULK_DISPLAY = "Bulk mode"
  val BULK_DEFAULT = false

  val KEY_PATH_CONFIG = s"$CONNECTOR_PREFIX.key.path"
  val KEY_PATH_DOC =
    s"When used with $KEY_SOURCE_CONFIG configurations of `KeyPath` or `ValuePath`, this is the path to the field in the object that will be used as the key. Defaults to 'id'."
  val KEY_PATH_DISPLAY = "Key path"
  val KEY_PATH_DEFAULT = "id"

  val CREATE_DATABASE_CONFIG = s"$CONNECTOR_PREFIX.$DATABASE_PROP_SUFFIX.create"
  val CREATE_DATABASE_DOC =
    "If set to true it will create the database if it doesn't exist. If this is set to default(false) an exception will be raised."
  val CREATE_DATABASE_DISPLAY = "Auto-create database"
  val CREATE_DATABASE_DEFAULT = false

  val PROXY_HOST_CONFIG  = s"$CONNECTOR_PREFIX.proxy"
  val PROXY_HOST_DOC     = "Specifies the connection proxy details."
  val PROXY_HOST_DISPLAY = "Proxy URI"

  val PROGRESS_COUNTER_ENABLED: String = PROGRESS_ENABLED_CONST
  val PROGRESS_COUNTER_ENABLED_DOC     = "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"

  val ErrorThresholdProp: String = s"$CONNECTOR_PREFIX.error.threshold"
  val ErrorThresholdDoc: String =
    """
      |The number of errors to tolerate before failing the sink.
      |""".stripMargin
  val ErrorThresholdDefault = 5

  val EnableFlushCountProp: String = s"$CONNECTOR_PREFIX.flush.count.enable"
  val EnableFlushCountDoc: String =
    """
      |Flush on count can be disabled by setting this property to 'false'.
      |""".stripMargin
  val EnableFlushCountDefault = true

  val UploadSyncPeriodProp: String = s"$CONNECTOR_PREFIX.upload.sync.period"
  val UploadSyncPeriodDoc: String =
    """
      |The time in milliseconds to wait before sending the request.
      |""".stripMargin
  val UploadSyncPeriodDefault = 100

  val ExecutorThreadsProp: String = s"$CONNECTOR_PREFIX.executor.threads"
  val ExecutorThreadsDoc: String =
    """
      |The number of threads to use for processing the records.
      |""".stripMargin
  val ExecutorThreadsDefault = 1

  val MaxQueueSizeProp: String = s"$CONNECTOR_PREFIX.max.queue.size"
  val MaxQueueSizeDoc: String =
    """
      |The maximum number of records to queue per topic before blocking. If the queue limit is reached the connector will throw RetriableException and the connector settings to handle retries will be used.
      |""".stripMargin
  val MaxQueueSizeDefault = 1000000

  val MaxQueueOfferTimeoutProp: String = s"$CONNECTOR_PREFIX.max.queue.offer.timeout.ms"
  val MaxQueueOfferTimeoutDoc: String =
    """
      |The maximum time in milliseconds to wait for the queue to accept a record. If the queue does not accept the record within this time, the connector will throw RetriableException and the connector settings to handle retries will be used.
      |""".stripMargin
  val MaxQueueOfferTimeoutDefault = 120000

  // The default value of 400 RU/s is chosen because it is the minimum manual throughput allowed by Azure Cosmos DB for a new container.
  // This value is cost-effective for development, testing, and many production workloads, and is the industry standard default recommended by Microsoft.
  // See: https://learn.microsoft.com/en-us/azure/cosmos-db/provisioned-throughput
  val COLLECTION_THROUGHPUT_CONFIG = s"$CONNECTOR_PREFIX.collection.throughput"
  val COLLECTION_THROUGHPUT_DOC =
    "The manual throughput to provision for new Cosmos DB collections (RU/s). The default is 400 RU/s, which is the minimum allowed by Azure Cosmos DB and is cost-effective for most workloads. See: https://learn.microsoft.com/en-us/azure/cosmos-db/provisioned-throughput"
  val COLLECTION_THROUGHPUT_DEFAULT = 400

}
