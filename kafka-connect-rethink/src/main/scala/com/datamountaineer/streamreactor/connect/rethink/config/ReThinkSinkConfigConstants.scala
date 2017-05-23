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

package com.datamountaineer.streamreactor.connect.rethink.config

import com.datamountaineer.streamreactor.temp.const.TraitConfigConst._

object ReThinkSinkConfigConstants {
  val RETHINK_CONNECTOR_PREFIX = "connect.rethink"

  val RETHINK_HOST = s"$RETHINK_CONNECTOR_PREFIX.sink.host"
  private[config] val RETHINK_HOST_DOC = "Rethink server host."
  val RETHINK_HOST_DEFAULT = "localhost"
  val RETHINK_DB = s"$RETHINK_CONNECTOR_PREFIX.$DATABASE_PROP_SUFFIX"
  private[config] val RETHINK_DB_DEFAULT = "connect_rethink_sink"
  private[config] val RETHINK_DB_DOC = "The reThink database to write to and create tables in."
  val RETHINK_PORT = s"$RETHINK_CONNECTOR_PREFIX.sink.port"
  val RETHINK_PORT_DEFAULT = "28015"
  private[config] val RETHINK_PORT_DOC = "Client port of rethink server to connect to."

  val CONFLICT_ERROR = "error"
  val CONFLICT_REPLACE = "replace"
  val CONFLICT_UPDATE = "update"

  val EXPORT_ROUTE_QUERY = s"$RETHINK_CONNECTOR_PREFIX.$KCQL_PROP_SUFFIX"
  private[config] val EXPORT_ROUTE_QUERY_DOC = "KCQL expression describing field selection and routes."

  val ERROR_POLICY = s"$RETHINK_CONNECTOR_PREFIX.$ERROR_POLICY_PROP_SUFFIX"
  private[config] val ERROR_POLICY_DOC: String = "Specifies the action to be taken if an error occurs while inserting the data.\n" +
    "There are two available options: \n" + "NOOP - the error is swallowed \n" +
    "THROW - the error is allowed to propagate. \n" +
    "RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on \n" +
    "The error will be logged automatically"
  private[config] val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL = s"$RETHINK_CONNECTOR_PREFIX.$RETRY_INTERVAL_PROP_SUFFIX"
  private[config] val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  private[config] val ERROR_RETRY_INTERVAL_DEFAULT = "60000"

  val NBR_OF_RETRIES = s"$RETHINK_CONNECTOR_PREFIX.$MAX_RETRIES_PROP_SUFFIX"
  private[config] val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  private[config] val NBR_OF_RETIRES_DEFAULT = 20

  val BATCH_SIZE = s"$RETHINK_CONNECTOR_PREFIX.$BATCH_SIZE_PROP_SUFFIX"
  private[config] val BATCH_SIZE_DOC = "Per topic the number of sink records to batch together and insert into ReThinkDB."
  private[config] val BATCH_SIZE_DEFAULT = 1000
}
