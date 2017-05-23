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

import com.datamountaineer.streamreactor.temp.const.TraitConfigConst.{DATABASE_PROP_SUFFIX, KCQL_PROP_SUFFIX}

object ReThinkSourceConfigConstants {
  val RETHINK_CONNECTOR_PREFIX = "connect.rethink"

  val RETHINK_HOST = s"$RETHINK_CONNECTOR_PREFIX.source.host"
  private[config] val RETHINK_HOST_DOC = "Rethink server host."
  val RETHINK_HOST_DEFAULT = "localhost"
  val RETHINK_DB = s"$RETHINK_CONNECTOR_PREFIX.$DATABASE_PROP_SUFFIX"
  private[config] val RETHINK_DB_DEFAULT = "connect_rethink_sink"
  private[config] val RETHINK_DB_DOC = "The reThink database to read from."
  val RETHINK_PORT = s"$RETHINK_CONNECTOR_PREFIX.source.port"
  val RETHINK_PORT_DEFAULT = "28015"
  private[config] val RETHINK_PORT_DOC = "Client port of rethink server to connect to."
  val IMPORT_ROUTE_QUERY = s"$RETHINK_CONNECTOR_PREFIX.$KCQL_PROP_SUFFIX"
}
