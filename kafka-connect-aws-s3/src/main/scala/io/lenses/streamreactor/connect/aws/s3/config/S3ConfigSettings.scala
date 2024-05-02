/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.aws.s3.config

import io.lenses.streamreactor.common.config.base.const.TraitConfigConst._

object S3ConfigSettings {

  val CONNECTOR_PREFIX = "connect.s3"

  val AWS_REGION:                  String = s"$CONNECTOR_PREFIX.aws.region"
  val AWS_ACCESS_KEY:              String = s"$CONNECTOR_PREFIX.aws.access.key"
  val AWS_SECRET_KEY:              String = s"$CONNECTOR_PREFIX.aws.secret.key"
  val AUTH_MODE:                   String = s"$CONNECTOR_PREFIX.aws.auth.mode"
  val CUSTOM_ENDPOINT:             String = s"$CONNECTOR_PREFIX.custom.endpoint"
  val ENABLE_VIRTUAL_HOST_BUCKETS: String = s"$CONNECTOR_PREFIX.vhost.bucket"

  val KCQL_CONFIG = s"$CONNECTOR_PREFIX.$KCQL_PROP_SUFFIX"
  val KCQL_DOC =
    "Contains the Kafka Connect Query Language describing the flow from Apache Kafka topics to Apache Hive tables."

  val HTTP_ERROR_RETRY_INTERVAL = s"$CONNECTOR_PREFIX.http.$RETRY_INTERVAL_PROP_SUFFIX"
  val HTTP_ERROR_RETRY_INTERVAL_DOC =
    "If greater than zero, used to determine the delay after which to retry the http request in milliseconds.  Based on an exponential backoff algorithm."
  val HTTP_ERROR_RETRY_INTERVAL_DEFAULT: Long = 50L

  val HTTP_NBR_OF_RETRIES = s"$CONNECTOR_PREFIX.http.$MAX_RETRIES_PROP_SUFFIX"
  val HTTP_NBR_OF_RETRIES_DOC =
    "Number of times to retry the http request, in the case of a resolvable error on the server side."
  val HTTP_NBR_OF_RETIRES_DEFAULT: Int = 5

  val HTTP_SOCKET_TIMEOUT         = s"$CONNECTOR_PREFIX.http.socket.timeout"
  val HTTP_SOCKET_TIMEOUT_DOC     = "Socket timeout (ms)"
  val HTTP_SOCKET_TIMEOUT_DEFAULT = 60000L

  val HTTP_CONNECTION_TIMEOUT         = s"$CONNECTOR_PREFIX.http.connection.timeout"
  val HTTP_CONNECTION_TIMEOUT_DOC     = "Connection timeout (ms)"
  val HTTP_CONNECTION_TIMEOUT_DEFAULT = 60000

  val SEEK_MAX_INDEX_FILES = s"$CONNECTOR_PREFIX.seek.max.files"
  val SEEK_MAX_INDEX_FILES_DOC =
    s"Maximum index files to allow per topic/partition.  Advisable to not raise this: if a large number of files build up this means there is a problem with file deletion."
  val SEEK_MAX_INDEX_FILES_DEFAULT = 5

  val POOL_MAX_CONNECTIONS     = s"$CONNECTOR_PREFIX.pool.max.connections"
  val POOL_MAX_CONNECTIONS_DOC = "Max connections in pool.  -1: Use default according to underlying client."
  val POOL_MAX_CONNECTIONS_DEFAULT: Int = -1

  val SOURCE_ORDERING_TYPE:         String = s"$CONNECTOR_PREFIX.ordering.type"
  val SOURCE_ORDERING_TYPE_DOC:     String = "AlphaNumeric (the default)"
  val SOURCE_ORDERING_TYPE_DEFAULT: String = "AlphaNumeric"

  // used by the consumer groups sink
  val S3_BUCKET_CONFIG: String = s"$CONNECTOR_PREFIX.location"
  val S3_BUCKET_DOC: String =
    "Specify the S3 bucket, and optionally, a prefix, where Kafka consumer group offsets will be stored."
}
