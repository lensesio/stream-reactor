/*
 * Copyright 2020 Lenses.io
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

import com.datamountaineer.streamreactor.common.config.base.const.TraitConfigConst._

object S3ConfigSettings {

  val CONNECTOR_PREFIX = "connect.s3"

  // Deprecated and will be removed in future
  val DEP_AWS_ACCESS_KEY:              String = "aws.access.key"
  val DEP_AWS_SECRET_KEY:              String = "aws.secret.key"
  val DEP_AUTH_MODE:                   String = "aws.auth.mode"
  val DEP_CUSTOM_ENDPOINT:             String = "aws.custom.endpoint"
  val DEP_ENABLE_VIRTUAL_HOST_BUCKETS: String = "aws.vhost.bucket"

  val AWS_REGION:                  String = s"$CONNECTOR_PREFIX.aws.region"
  val AWS_ACCESS_KEY:              String = s"$CONNECTOR_PREFIX.aws.access.key"
  val AWS_SECRET_KEY:              String = s"$CONNECTOR_PREFIX.aws.secret.key"
  val AUTH_MODE:                   String = s"$CONNECTOR_PREFIX.aws.auth.mode"
  val CUSTOM_ENDPOINT:             String = s"$CONNECTOR_PREFIX.custom.endpoint"
  val ENABLE_VIRTUAL_HOST_BUCKETS: String = s"$CONNECTOR_PREFIX.vhost.bucket"
  val AWS_CLIENT:                  String = s"$CONNECTOR_PREFIX.aws.client"

  val DISABLE_FLUSH_COUNT: String = s"$CONNECTOR_PREFIX.disable.flush.count"
  val LOCAL_TMP_DIRECTORY: String = s"$CONNECTOR_PREFIX.local.tmp.directory"

  val PROFILES: String = s"$CONNECTOR_PREFIX.config.profiles"

  val KCQL_BUILDER = s"$CONNECTOR_PREFIX.$KCQL_PROP_SUFFIX.builder"
  val KCQL_CONFIG  = s"$CONNECTOR_PREFIX.$KCQL_PROP_SUFFIX"
  val KCQL_DOC =
    "Contains the Kafka Connect Query Language describing the flow from Apache Kafka topics to Apache Hive tables."
  val KCQL_DISPLAY = "KCQL commands"

  val PROGRESS_COUNTER_ENABLED: String = PROGRESS_ENABLED_CONST
  val PROGRESS_COUNTER_ENABLED_DOC     = "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"

  val ERROR_POLICY = s"$CONNECTOR_PREFIX.$ERROR_POLICY_PROP_SUFFIX"
  val ERROR_POLICY_DOC =
    """
      |Specifies the action to be taken if an error occurs while inserting the data.
      | There are three available options:
      |    NOOP - the error is swallowed
      |    THROW - the error is allowed to propagate.
      |    RETRY - The exception causes the Connect framework to retry the message. The number of retries is set by connect.s3.max.retries.
      |All errors will be logged automatically, even if the code swallows them.
    """.stripMargin
  val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL     = s"$CONNECTOR_PREFIX.$RETRY_INTERVAL_PROP_SUFFIX"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT: Long = 60000L

  val NBR_OF_RETRIES     = s"$CONNECTOR_PREFIX.$MAX_RETRIES_PROP_SUFFIX"
  val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT: Int = 20

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

  val SEEK_MIGRATION = s"$CONNECTOR_PREFIX.seek.migration.enabled"
  val SEEK_MIGRATION_DOC =
    "Migration mode, from legacy to optimised sink seek method.  This will be deprecated and removed in a future version."
  val SEEK_MIGRATION_DEFAULT = false

  val SEEK_MAX_INDEX_FILES = s"$CONNECTOR_PREFIX.seek.max.files"
  val SEEK_MAX_INDEX_FILES_DOC =
    s"Maximum index files to allow per topic/partition.  Advisable to not raise this: if a large number of files build up this means there is a problem with file deletion."
  val SEEK_MAX_INDEX_FILES_DEFAULT = 5

  val SOURCE_PARTITION_EXTRACTOR_TYPE = s"$CONNECTOR_PREFIX.source.partition.extractor.type"
  val SOURCE_PARTITION_EXTRACTOR_TYPE_DOC =
    "If you want to read to specific partitions when running the source.  Options are 'hierarchical' (to match the sink's hierarchical file storage pattern) and 'regex' (supply a custom regex).  Any other value will ignore original partitions and they should be evenly distributed through available partitions (Kafka dependent)."

  val SOURCE_PARTITION_EXTRACTOR_REGEX     = s"$CONNECTOR_PREFIX.source.partition.extractor.regex"
  val SOURCE_PARTITION_EXTRACTOR_REGEX_DOC = "If reading filename from regex, supply the regex here."

  val POOL_MAX_CONNECTIONS     = s"$CONNECTOR_PREFIX.pool.max.connections"
  val POOL_MAX_CONNECTIONS_DOC = "Max connections in pool.  -1: Use default according to underlying client."
  val POOL_MAX_CONNECTIONS_DEFAULT: Int = -1

}
