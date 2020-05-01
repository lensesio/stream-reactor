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

package com.datamountaineer.streamreactor.connect.voltdb.config

import com.datamountaineer.kcql.WriteModeEnum
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ThrowErrorPolicy}
import com.datamountaineer.streamreactor.connect.voltdb.StructFieldsExtractor
import org.apache.kafka.common.config.ConfigException

import scala.collection.JavaConverters._


case class VoltSettings(servers: String,
                        user: String,
                        password: String,
                        fieldsExtractorMap: Map[String, StructFieldsExtractor],
                        errorPolicy: ErrorPolicy = new ThrowErrorPolicy,
                        maxRetries: Int = VoltSinkConfigConstants.NBR_OF_RETIRES_DEFAULT)

object VoltSettings {

  /**
    * Creates an instance of InfluxSettings from a VoltSinkConfig
    *
    * @param config : The map of all provided configurations
    * @return An instance of InfluxSettings
    */
  def apply(config: VoltSinkConfig): VoltSettings = {
    val servers = config.getString(VoltSinkConfigConstants.SERVERS_CONFIG)

    if (servers == null || servers.trim.length == 0) {
      throw new ConfigException(s"${VoltSinkConfigConstants.SERVERS_CONFIG} is not set correctly")
    }

    val user = config.getUsername
    val passwordRaw = config.getSecret

    val password = passwordRaw match {
      case null => null
      case _ => passwordRaw.value()
    }

    val kcql = config.getKCQL
    val errorPolicy = config.getErrorPolicy
    val nbrOfRetries = config.getNumberRetries
    val fields = config.getFieldsMap()

    val extractorFields = kcql.map { rm =>
      val allFields = rm.getFields.asScala.exists(_.getName.equals("*"))
      (rm.getSource, StructFieldsExtractor(rm.getTarget, allFields, fields(rm.getSource), rm.getWriteMode == WriteModeEnum.UPSERT))
    }.toMap

    new VoltSettings(servers,
      user,
      password,
      extractorFields,
      errorPolicy,
      nbrOfRetries)
  }
}
