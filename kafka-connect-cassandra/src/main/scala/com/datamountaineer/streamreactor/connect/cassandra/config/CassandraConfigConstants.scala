/*
 *  Copyright 2016 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.cassandra.config

/**
  * Created by andrew@datamountaineer.com on 25/04/16. 
  * stream-reactor
  */

/**
  * Holds the constants used in the config.
  **/

object CassandraConfigConstants {
  val USERNAME_PASSWORD = "username_password"
  val NONE = "none"
  val PAYLOAD = "payload"
  val BULK = "bulk"
  val INCREMENTAL = "incremental"
  val DEFAULT_POLL_INTERVAL = 60000L //1 minute

  val POLL_INTERVAL = "connect.cassandra.import.poll.interval"
  val POLL_INTERVAL_DOC = "The polling interval between queries against tables for bulk mode."

  val KEY_SPACE = "connect.cassandra.key.space"
  val KEY_SPACE_DOC = "Keyspace to write to."

  val CONTACT_POINTS = "connect.cassandra.contact.points"
  val CONTACT_POINT_DOC = "Initial contact point host for Cassandra including port."
  val CONTACT_POINT_DEFAULT = "localhost"

  val PORT = "connect.cassandra.port"
  val PORT_DEFAULT = "9042"
  val PORT_DOC = "Cassandra native port."

  val INSERT_JSON_PREFIX = "INSERT INTO "
  val INSERT_JSON_POSTFIX = " JSON '?';"

  val USERNAME = "connect.cassandra.username"
  val USERNAME_DOC = "Username to connect to Cassandra with."
  val USERNAME_DEFAULT = "cassandra.cassandra"

  val PASSWD = "connect.cassandra.password"
  val PASSWD_DOC = "Password for the username to connect to Cassandra with."
  val PASSWD_DEFAULT = "cassandra"

  val SSL_ENABLED = "connect.cassandra.ssl.enabled"
  val SSL_ENABLED_DOC = "Secure Cassandra driver connection via SSL."
  val SSL_ENABLED_DEFAULT = "false"

  val TRUST_STORE_PATH = "connect.cassandra.trust.store.path"
  val TRUST_STORE_PATH_DOC = "Path to the client Trust store."

  val TRUST_STORE_PASSWD = "connect.cassandra.trust.store.password"
  val TRUST_STORE_PASSWD_DOC = "Password for the client Trust store."

  val USE_CLIENT_AUTH = "connect.cassandra.ssl.client.cert.auth"
  val USE_CLIENT_AUTH_DEFAULT = "false"
  val USE_CLIENT_AUTH_DOC = "Enable client certification authentication by Cassandra. Requires KeyStore options to be set."

  val KEY_STORE_PATH = "connect.cassandra.key.store.path"
  val KEY_STORE_PATH_DOC = "Path to the client Key store."
  val KEY_STORE_PASSWD = "connect.cassandra.key.store.password"
  val KEY_STORE_PASSWD_DOC = "Password for the client Key Store"

  //source
  val IMPORT_MODE = "connect.cassandra.import.mode"
  val IMPORT_MODE_DOC = s"Import mode for the tables. Either $BULK or $INCREMENTAL"

  val BATCH_SIZE = "connect.cassandra.source.task.batch.size"
  val BATCH_SIZE_DOC = "The number of records the source task should drain from the reader queue."
  val BATCH_SIZE_DEFAULT = 100

  val READER_BUFFER_SIZE = "connect.cassandra.source.task.buffer.size"
  val READER_BUFFER_SIZE_DOC = "The size of the queue as read writes to."
  val READER_BUFFER_SIZE_DEFAULT = 10000

  val ALLOW_FILTERING = "connect.cassandra.import.source.allow.filtering"
  val ALLOW_FILTERING_DOC = "Enable ALLOW FILTERING in incremental selects."
  val ALLOW_FILTERING_DEFAULT = true

  //for the source task, the connector will set this for the each source task
  val ASSIGNED_TABLES = "connect.cassandra.assigned.tables"
  val ASSIGNED_TABLES_DOC = "The tables a task has been assigned."

  val MISSING_KEY_SPACE_MESSAGE = s"$KEY_SPACE must be provided."
  val SELECT_OFFSET_COLUMN = "___kafka_connect_offset_col"

  val ERROR_POLICY = "connect.cassandra.error.policy"
  val ERROR_POLICY_DOC =
    """
      |Specifies the action to be taken if an error occurs while inserting the data.
      | There are three available options:
      |    NOOP - the error is swallowed
      |    THROW - the error is allowed to propagate.
      |    RETRY - The exception causes the Connect framework to retry the message. The number of retries is set by connect.cassandra.max.retires.
      |All errors will be logged automatically, even if the code swallows them.
    """.stripMargin
  val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL = "connect.cassandra.retry.interval"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"
  val NBR_OF_RETRIES = "connect.cassandra.max.retires"
  val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20


  val SINK_KCQL = "connect.cassandra.sink.kcql"
  val SINK_KCQL_DOC = "KCQL expression describing field selection and routes."

  val SOURCE_KCQL_QUERY = "connect.cassandra.source.kcql"
  val SOURCE_KCQL_DOC = "KCQL expression describing field selection and routes."

  val SINK_THREAD_POOL_CONFIG = "connect.cassandra.sink.threadpool.size"
  val SINK_THREAD_POOL_DOC =
    """
      |The sink inserts all the data concurrently. To fail fast in case of an error, the sink has its own thread pool.
      |Set the value to zero and the threadpool will default to 4* NO_OF_CPUs. Set a value greater than 0
      |and that would be the size of this threadpool.""".stripMargin
  val SINK_THREAD_POOL_DISPLAY = "Thread pool size"
  val SINK_THREAD_POOL_DEFAULT = 0

  val CONSISTENCY_LEVEL_CONFIG = "connect.cassandra.consistency.level"
  val CONSISTENCY_LEVEL_DOC =
    """
      |Consistency refers to how up-to-date and synchronized a row of Cassandra data is on all of its replicas.
      |Cassandra offers tunable consistency. For any given read or write operation, the client application decides how consistent the requested data must be.
    """.stripMargin
  val CONSISTENCY_LEVEL_DISPLAY = "Consistency Level"
  val CONSISTENCY_LEVEL_DEFAULT = ""

}
