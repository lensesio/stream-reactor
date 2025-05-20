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

import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.BATCH_SIZE_PROP_SUFFIX
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.CONSISTENCY_LEVEL_PROP_SUFFIX
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.ERROR_POLICY_PROP_SUFFIX
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.KCQL_PROP_SUFFIX
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.MAX_RETRIES_PROP_SUFFIX
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.PASSWORD_SUFFIX
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.PROGRESS_ENABLED_CONST
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.RETRY_INTERVAL_PROP_SUFFIX
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.THREAD_POLL_PROP_SUFFIX
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.USERNAME_SUFFIX

/**
  * Created by andrew@datamountaineer.com on 25/04/16.
  * stream-reactor
  */

/**
  * Holds the constants used in the config.
  */

object CassandraConfigConstants {
  val CONNECTOR_PREFIX = "connect.cassandra"

  val NONE                  = "none"
  val DEFAULT_POLL_INTERVAL = 1000L

  val POLL_INTERVAL     = s"$CONNECTOR_PREFIX.import.poll.interval"
  val POLL_INTERVAL_DOC = "The polling interval between queries against tables for bulk mode."

  val KEY_SPACE     = s"$CONNECTOR_PREFIX.key.space"
  val KEY_SPACE_DOC = "Keyspace to write to."

  val CONTACT_POINTS        = s"$CONNECTOR_PREFIX.contact.points"
  val CONTACT_POINT_DOC     = "Initial contact point host for Cassandra including port."
  val CONTACT_POINT_DEFAULT = "localhost"

  val PORT         = s"$CONNECTOR_PREFIX.port"
  val PORT_DEFAULT = "9042"
  val PORT_DOC     = "Cassandra native port."

  val USERNAME     = s"$CONNECTOR_PREFIX.$USERNAME_SUFFIX"
  val USERNAME_DOC = "Username to connect to Cassandra with."

  val PASSWD     = s"$CONNECTOR_PREFIX.$PASSWORD_SUFFIX"
  val PASSWD_DOC = "Password for the username to connect to Cassandra with."

  val SSL_ENABLED         = s"$CONNECTOR_PREFIX.ssl.enabled"
  val SSL_ENABLED_DOC     = "Secure Cassandra driver connection via SSL."
  val SSL_ENABLED_DEFAULT = "false"

  val TRUST_STORE_PATH         = s"$CONNECTOR_PREFIX.trust.store.path"
  val TRUST_STORE_PATH_DOC     = "Path to the client Trust Store."
  val TRUST_STORE_PASSWD       = s"$CONNECTOR_PREFIX.trust.store.password"
  val TRUST_STORE_PASSWD_DOC   = "Password for the client Trust Store."
  val TRUST_STORE_TYPE         = s"$CONNECTOR_PREFIX.trust.store.type"
  val TRUST_STORE_TYPE_DOC     = "Type of the Trust Store, defaults to JKS"
  val TRUST_STORE_TYPE_DEFAULT = "JKS"

  val USE_CLIENT_AUTH         = s"$CONNECTOR_PREFIX.ssl.client.cert.auth"
  val USE_CLIENT_AUTH_DEFAULT = "false"
  val USE_CLIENT_AUTH_DOC =
    "Enable client certification authentication by Cassandra. Requires KeyStore options to be set."

  val KEY_STORE_PATH         = s"$CONNECTOR_PREFIX.key.store.path"
  val KEY_STORE_PATH_DOC     = "Path to the client Key Store."
  val KEY_STORE_PASSWD       = s"$CONNECTOR_PREFIX.key.store.password"
  val KEY_STORE_PASSWD_DOC   = "Password for the client Key Store"
  val KEY_STORE_TYPE         = s"$CONNECTOR_PREFIX.key.store.type"
  val KEY_STORE_TYPE_DOC     = "Type of the Key Store, defauts to JKS"
  val KEY_STORE_TYPE_DEFAULT = "JKS"

  //source

  val BATCH_SIZE         = s"$CONNECTOR_PREFIX.$BATCH_SIZE_PROP_SUFFIX"
  val BATCH_SIZE_DOC     = "The number of records the source task should drain from the reader queue."
  val BATCH_SIZE_DEFAULT = 100

  val FETCH_SIZE         = s"$CONNECTOR_PREFIX.fetch.size"
  val FETCH_SIZE_DOC     = "The number of records the Cassandra driver will return at once."
  val FETCH_SIZE_DEFAULT = 5000

  val READER_BUFFER_SIZE         = s"$CONNECTOR_PREFIX.task.buffer.size"
  val READER_BUFFER_SIZE_DOC     = "The size of the queue as read writes to."
  val READER_BUFFER_SIZE_DEFAULT = 10000

  val TIME_SLICE_MILLIS = s"$CONNECTOR_PREFIX.time.slice.ms"
  val TIME_SLICE_MILLIS_DOC =
    "The range of time in milliseconds the source task the timestamp/timeuuid will use for query"
  val TIME_SLICE_MILLIS_DEFAULT = 10000L

  val ALLOW_FILTERING         = s"$CONNECTOR_PREFIX.import.allow.filtering"
  val ALLOW_FILTERING_DOC     = "Enable ALLOW FILTERING in incremental selects."
  val ALLOW_FILTERING_DEFAULT = true

  //for the source task, the connector will set this for the each source task
  val ASSIGNED_TABLES     = s"$CONNECTOR_PREFIX.assigned.tables"
  val ASSIGNED_TABLES_DOC = "The tables a task has been assigned."

  val MISSING_KEY_SPACE_MESSAGE = s"$KEY_SPACE must be provided."

  val ERROR_POLICY = s"$CONNECTOR_PREFIX.$ERROR_POLICY_PROP_SUFFIX"
  val ERROR_POLICY_DOC =
    """
      |Specifies the action to be taken if an error occurs while inserting the data.
      | There are three available options:
      |    NOOP - the error is swallowed
      |    THROW - the error is allowed to propagate.
      |    RETRY - The exception causes the Connect framework to retry the message. The number of retries is set by connect.cassandra.max.retries.
      |All errors will be logged automatically, even if the code swallows them.
    """.stripMargin
  val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL         = s"$CONNECTOR_PREFIX.$RETRY_INTERVAL_PROP_SUFFIX"
  val ERROR_RETRY_INTERVAL_DOC     = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"

  val NBR_OF_RETRIES         = s"$CONNECTOR_PREFIX.$MAX_RETRIES_PROP_SUFFIX"
  val NBR_OF_RETRIES_DOC     = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20

  val KCQL     = s"$CONNECTOR_PREFIX.$KCQL_PROP_SUFFIX"
  val KCQL_DOC = "KCQL expression describing field selection and routes."

  val THREAD_POOL_CONFIG = s"$CONNECTOR_PREFIX.$THREAD_POLL_PROP_SUFFIX"
  val THREAD_POOL_DOC =
    """
      |The sink inserts all the data concurrently. To fail fast in case of an error, the sink has its own thread pool.
      |Set the value to zero and the threadpool will default to 4* NO_OF_CPUs. Set a value greater than 0
      |and that would be the size of this threadpool.""".stripMargin
  val THREAD_POOL_DISPLAY = "Thread pool size"
  val THREAD_POOL_DEFAULT = 0

  val CONSISTENCY_LEVEL_CONFIG = s"$CONNECTOR_PREFIX.$CONSISTENCY_LEVEL_PROP_SUFFIX"
  val CONSISTENCY_LEVEL_DOC =
    """
      |Consistency refers to how up-to-date and synchronized a row of Cassandra data is on all of its replicas.
      |Cassandra offers tunable consistency. For any given read or write operation, the client application decides how consistent the requested data must be.
    """.stripMargin
  val CONSISTENCY_LEVEL_DISPLAY = "Consistency Level"
  val CONSISTENCY_LEVEL_DEFAULT = ""

  val PROGRESS_COUNTER_ENABLED         = PROGRESS_ENABLED_CONST
  val PROGRESS_COUNTER_ENABLED_DOC     = "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"

  val DELETE_ROW_PREFIX = "delete"

  val DELETE_ROW_ENABLED         = s"$CONNECTOR_PREFIX.$DELETE_ROW_PREFIX.enabled"
  val DELETE_ROW_ENABLED_DOC     = "Enables row deletion from cassandra"
  val DELETE_ROW_ENABLED_DEFAULT = false
  val DELETE_ROW_ENABLED_DISPLAY = "Enable delete row"

  val DELETE_ROW_STATEMENT         = s"$CONNECTOR_PREFIX.$DELETE_ROW_PREFIX.statement"
  val DELETE_ROW_STATEMENT_DOC     = "Delete statement for cassandra"
  val DELETE_ROW_STATEMENT_DEFAULT = ""
  val DELETE_ROW_STATEMENT_DISPLAY = "Enable delete row"
  val DELETE_ROW_STATEMENT_MISSING = s"If $DELETE_ROW_ENABLED is true, $DELETE_ROW_STATEMENT is required."

  val DELETE_ROW_STRUCT_FLDS = s"$CONNECTOR_PREFIX.$DELETE_ROW_PREFIX.struct_flds"
  val DELETE_ROW_STRUCT_FLDS_DOC =
    s"Fields in the key struct data type used in there delete statement. Comma-separated in the order they are found in $DELETE_ROW_STATEMENT. Keep default value to use the record key as a primitive type."
  val DELETE_ROW_STRUCT_FLDS_DEFAULT = ""
  val DELETE_ROW_STRUCT_FLDS_DISPLAY = "Field names in Key Struct"

  val TIMESLICE_DURATION         = s"$CONNECTOR_PREFIX.slice.duration"
  val TIMESLICE_DURATION_DEFAULT = 10000L
  val TIMESLICE_DURATION_DOC     = "Duration to query for in target Cassandra table. Used to restrict query timestamp span"

  val TIMESLICE_DELAY         = s"$CONNECTOR_PREFIX.slice.delay.ms"
  val TIMESLICE_DELAY_DEFAULT = 30000L
  val TIMESLICE_DELAY_DOC =
    "The delay between the current time and the time range of the query. Used to insure all of the data in the time slice is available"

  val INITIAL_OFFSET         = s"$CONNECTOR_PREFIX.initial.offset"
  val INITIAL_OFFSET_DEFAULT = "1900-01-01 00:00:00.0000000Z"
  val INITIAL_OFFSET_DOC =
    "The initial timestamp to start querying in Cassandra from (yyyy-MM-dd HH:mm:ss.SSS'Z'). Default 1900-01-01 00:00:00.0000000Z"

  val MAPPING_COLLECTION_TO_JSON         = s"$CONNECTOR_PREFIX.mapping.collection.to.json"
  val MAPPING_COLLECTION_TO_JSON_DOC     = "Mapping columns with type Map, List and Set like json"
  val MAPPING_COLLECTION_TO_JSON_DEFAULT = true

  val BUCKET_TIME_SERIES_MODE     = s"$CONNECTOR_PREFIX.bucket.timeseries.mode"
  val BUCKET_TIME_SERIES_MODE_DOC = "The bucket mode to retrieve timeseries data. DAY, HOUR, MINUTE, SECOND."

  val BUCKET_TIME_SERIES_FORMAT     = s"$CONNECTOR_PREFIX.bucket.timeseries.format"
  val BUCKET_TIME_SERIES_FORMAT_DOC = "The bucket format to retrieve timeseries data"

  val BUCKET_TIME_SERIES_FIELD_NAME         = s"$CONNECTOR_PREFIX.bucket.timeseries.field.name"
  val BUCKET_TIME_SERIES_FIELD_NAME_DOC     = "The name of the field to use on bucket"
  val BUCKET_TIME_SERIES_FIELD_NAME_DEFAULT = "bucket"

  val DEFAULT_VALUE_SERVE_STRATEGY_PROPERTY = s"$CONNECTOR_PREFIX.default.value"
  val DEFAULT_VALUE_SERVE_STRATEGY_DOC =
    """
      |By default a column omitted from the ``JSON`` map will be set to ``NULL``.
      |Alternatively, if set ``UNSET``, pre-existing value  will be preserved.
    """.stripMargin

  val DEFAULT_VALUE_SERVE_STRATEGY_DEFAULT = ""
  val DEFAULT_VALUE_SERVE_STRATEGY_DISPLAY = "Default value serve strategy"

  val LOAD_BALANCING_POLICY = s"$CONNECTOR_PREFIX.load.balancing.policy"
  val LOAD_BALANCING_POLICY_DOC =
    "Cassandra Load balancing policy. ROUND_ROBIN, TOKEN_AWARE, LATENCY_AWARE or DC_AWARE_ROUND_ROBIN. TOKEN_AWARE and LATENCY_AWARE use DC_AWARE_ROUND_ROBIN"

  val CONNECT_TIMEOUT         = s"$CONNECTOR_PREFIX.connect.timeout.ms"
  val CONNECT_TIMEOUT_DOC     = "The Cassandra driver connection timeout in milliseconds."
  val DEFAULT_CONNECT_TIMEOUT = 5000

  val READ_TIMEOUT         = s"$CONNECTOR_PREFIX.read.timeout.ms"
  val READ_TIMEOUT_DOC     = "The Cassandra driver read timeout in milliseconds."
  val DEFAULT_READ_TIMEOUT = 12000

}
