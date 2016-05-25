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

package com.datamountaineer.streamreactor.connect.cassandra.config

/**
  * Created by andrew@datamountaineer.com on 25/04/16. 
  * stream-reactor
  */

/**
  * Holds the constants used in the config.
  * */

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
  val KEY_SPACE_DOC ="Keyspace to write to."

  val CONTACT_POINTS = "connect.cassandra.contact.points"
  val CONTACT_POINT_DOC ="Initial contact point host for Cassandra including port."
  val CONTACT_POINT_DEFAULT = "cassandra.localhost"

  val PORT = "connect.cassandra.port"
  val PORT_DEFAULT = "9042"
  val PORT_DOC ="Cassandra native port."

  val INSERT_JSON_PREFIX = "INSERT INTO "
  val INSERT_JSON_POSTFIX = " JSON '?';"

  val AUTHENTICATION_MODE = "connect.cassandra.authentication.mode"
  val AUTHENTICATION_MODE_DOC ="Authentication mode. NONE, USERNAME_PASSWORD"
  val AUTHENTICATION_MODE_DEFAULT = CassandraConfigConstants.NONE

  val USERNAME = "connect.cassandra.username"
  val USERNAME_DOC ="Username to connect to Cassandra with."
  val USERNAME_DEFAULT = "cassandra.cassandra"

  val PASSWD = "connect.cassandra.password"
  val PASSWD_DOC ="Password for the username to connect to Cassandra with."
  val PASSWD_DEFAULT = "cassandra"

  val SSL_ENABLED = "connect.cassandra.ssl.enabled"
  val SSL_ENABLED_DOC ="Secure Cassandra driver connection via SSL."
  val SSL_ENABLED_DEFAULT = "false"

  val TRUST_STORE_PATH = "connect.cassandra.trust.store.path"
  val TRUST_STORE_PATH_DOC ="Path to the client Trust store."

  val TRUST_STORE_PASSWD = "connect.cassandra.trust.store.password"
  val TRUST_STORE_PASSWD_DOC ="Password for the client Trust store."

  val USE_CLIENT_AUTH = "connect.cassandra.ssl.client.cert.auth"
  val USE_CLIENT_AUTH_DEFAULT = "false"
  val USE_CLIENT_AUTH_DOC ="Enable client certification authentication by Cassandra. Requires KeyStore options to be set."

  val KEY_STORE_PATH = "connect.cassandra.key.store.path"
  val KEY_STORE_PATH_DOC ="Path to the client Key store."
  val KEY_STORE_PASSWD = "connect.cassandra.key.store.password"
  val KEY_STORE_PASSWD_DOC ="Password for the client Key Store"

  //source
  val IMPORT_MODE = "connect.cassandra.import.mode"
  val IMPORT_MODE_DOC =s"Import mode for the tables. Either $BULK or $INCREMENTAL"

  val IMPORT_TABLE_TOPIC_MAP = "connect.cassandra.import.map"
  val IMPORT_TABLE_TOPIC_MAP_DOC ="Table to Topic map for import in format table1=topic1,table2=topic2, if the topic left blank table name is used."

  val EXPORT_TABLE_TOPIC_MAP = "connect.cassandra.export.map"
  val EXPORT_TABLE_TOPIC_MAP_DOC = "Topic to Table map for import in format topic1:table1, if the table left blank topic name is used."

  val TABLE_TIMESTAMP_COL_MAP = "connect.cassandra.import.timestamp.column"
  val TABLE_TIMESTAMP_COL_MAP_DOC =
    "Name of the timestamp column in the cassandra table to use identify deltas. table1:col,table2:col."

  val BATCH_SIZE = "connect.cassandra.source.task.batch.size"
  val BATCH_SIZE_DOC ="The number of records the source task should drain from the reader queue."
  val BATCH_SIZE_DEFAULT = 100

  val FETCH_SIZE = "connect.cassandra.import.fetch.size"
  val FETCH_SIZE_DOC = "The fetch size for the Cassandra driver to read."
  val FETCH_SIZE_DEFAULT = 1000

  val READER_BUFFER_SIZE = "connect.cassandra.source.task.buffer.size"
  val READER_BUFFER_SIZE_DOC = "The size of the queue as read writes to."
  val READER_BUFFER_SIZE_DEFAULT = 10000

  val ALLOW_FILTERING = "connect.cassandra.import.source.allow.filtering"
  val ALLOW_FILTERING_DOC = "Enable ALLOW FILTERING in incremental selects."
  val ALLOW_FILTERING_DEFAULT = true

  //for the source task, the connector will set this for the each source task
  val ASSIGNED_TABLES = "connect.cassandra.assigned.tables"
  val ASSIGNED_TABLES_DOC = "The tables a task has been assigned."

  val EMPTY_EXPORT_MAP_MESSAGE = s"$EXPORT_TABLE_TOPIC_MAP can not be empty."
  val MISSING_ASSIGNED_TOPIC_IN_MAP_MESSAGE = s"Assigned topics for this task not in $EXPORT_TABLE_TOPIC_MAP."

  val EMPTY_IMPORT_MAP_MESSAGE = s"$IMPORT_TABLE_TOPIC_MAP can not be empty."
  val MISSING_ASSIGNED_TABLE_IN_MAP_MESSAGE = s"Assigned tables for this task not in $IMPORT_TABLE_TOPIC_MAP."

  val MISSING_KEY_SPACE_MESSAGE = s"$KEY_SPACE must be provided."
  val SELECT_OFFSET_COLUMN = "___kafka_connect_offset_col"
}
