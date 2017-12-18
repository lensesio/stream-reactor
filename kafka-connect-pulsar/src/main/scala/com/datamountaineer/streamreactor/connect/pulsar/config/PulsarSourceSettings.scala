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

package com.datamountaineer.streamreactor.connect.pulsar.config

import com.datamountaineer.streamreactor.connect.converters.source.{BytesConverter, Converter}
import org.apache.kafka.common.config.ConfigException

import scala.util.{Failure, Success, Try}

case class PulsarSourceSettings(connection: String,
                                sourcesToConverters: Map[String, String],
                                throwOnConversion: Boolean,
                                kcql: Array[String],
                                pollingTimeout: Int,
                                enableProgress: Boolean = PulsarConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT) {

  def asMap(): java.util.Map[String, String] = {
    val map = new java.util.HashMap[String, String]()
    map.put(PulsarConfigConstants.HOSTS_CONFIG, connection)
    //user.foreach(u => map.put(PulsarConfigConstants.USER_CONFIG, u))
    //password.foreach(p => map.put(PulsarConfigConstants.PASSWORD_CONFIG, p))
    map.put(PulsarConfigConstants.POLLING_TIMEOUT_CONFIG, pollingTimeout.toString)
    map.put(PulsarConfigConstants.KCQL_CONFIG, kcql.mkString(";"))
    map
  }
}


object PulsarSourceSettings {
  def apply(config: PulsarSourceConfig): PulsarSourceSettings = {
    def getFile(configKey: String) = Option(config.getString(configKey))

    val kcql = config.getKCQL
    val kcqlStr = config.getKCQLRaw
//    val user = Some(config.getUsername)
//    val password = Option(config.getSecret).map(_.value())
    val connection = config.getHosts

    val progressEnabled = config.getBoolean(PulsarConfigConstants.PROGRESS_COUNTER_ENABLED)

    val converters = kcql.map(k => {
      (k.getSource, if (k.getWithConverter == null) classOf[BytesConverter].getCanonicalName else k.getWithConverter)
    }).toMap

    converters.foreach { case (pulsar_source, clazz) =>
      Try(getClass.getClassLoader.loadClass(clazz)) match {
        case Failure(_) => throw new ConfigException(s"Invalid ${PulsarConfigConstants.KCQL_CONFIG}. $clazz can't be found for $pulsar_source")
        case Success(clz) =>
          if (!classOf[Converter].isAssignableFrom(clz)) {
            throw new ConfigException(s"Invalid ${PulsarConfigConstants.KCQL_CONFIG}. $clazz is not inheriting Converter for $pulsar_source")
          }
      }
    }

    PulsarSourceSettings(
      connection,
      //user,
      //password,
      converters,
      config.getBoolean(PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG),
      kcqlStr,
      config.getInt(PulsarConfigConstants.POLLING_TIMEOUT_CONFIG),
      progressEnabled
    )
  }
}
