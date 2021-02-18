/*
 *
 *  * Copyright 2020 Lenses.io.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.datamountaineer.streamreactor.common.config.base.traits

/**
  * Created by andrew@datamountaineer.com on 31/07/2017. 
  * stream-reactor
  */

import com.datamountaineer.streamreactor.common.config.base.const.TraitConfigConst._
import org.apache.kafka.common.config.ConfigException

trait ConnectionSettings extends BaseSettings {
  val uriConst = s"$connectorPrefix.$URI_SUFFIX"
  val schemaRegistryConst = s"$connectorPrefix.$SCHEMA_REGISTRY_SUFFIX"
  val urlConst = s"$connectorPrefix.$URL_SUFFIX"
  val hostConst = s"$connectorPrefix.$CONNECTION_HOST_SUFFIX"
  val hostsConst = s"$connectorPrefix.$CONNECTION_HOSTS_SUFFIX"
  val portConst = s"$connectorPrefix.$CONNECTION_PORT_SUFFIX"
  val portsConst = s"$connectorPrefix.$CONNECTION_PORTS_SUFFIX"

  def getPort = getInt(portConst)
  def getUri = getString(uriConst)
  def getSchemaRegistryUrl = getString(schemaRegistryConst)

  def getUrl : String = {
    val url = getString(urlConst)
    if (url == null || url.trim.length == 0) {
      throw new ConfigException(s"$urlConst has not been set")
    }
    url
  }

  def getHosts : String = {
    val connection = getString(hostsConst)

    if (connection == null || connection.trim.isEmpty) {
      throw new ConfigException(s"$hostsConst is not provided!")
    }
    connection
  }

  def getHost : String = {
    val connection = getString(hostConst)

    if (connection == null || connection.trim.isEmpty) {
      throw new ConfigException(s"$hostsConst is not provided!")
    }
    connection
  }
}
