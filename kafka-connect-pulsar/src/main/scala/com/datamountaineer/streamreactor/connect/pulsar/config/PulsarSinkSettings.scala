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

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ThrowErrorPolicy}
import org.apache.kafka.common.config.ConfigException


/**
  * Created by andrew@datamountaineer.com on 27/08/2017. 
  * stream-reactor
  */
case class PulsarSinkSettings(connection: String,
                              kcql : Set[Kcql],
                              enableProgress : Boolean = PulsarConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
                              errorPolicy: ErrorPolicy = new ThrowErrorPolicy,
                              maxRetries: Int = PulsarConfigConstants.NBR_OF_RETIRES_DEFAULT
                           )

object PulsarSinkSettings {
  def apply(config: PulsarSinkConfig): PulsarSinkSettings = {
    def getFile(configKey: String) = Option(config.getString(configKey))

    val kcql = config.getKCQL
    //val user = Some(config.getUsername)
    //val password =  Option(config.getSecret).map(_.value())
    val connection = config.getHosts

    val progressEnabled = config.getBoolean(PulsarConfigConstants.PROGRESS_COUNTER_ENABLED)

    val errorPolicy = config.getErrorPolicy
    val maxRetries = config.getNumberRetries

    new PulsarSinkSettings(
      connection,
      kcql,
      progressEnabled,
      errorPolicy,
      maxRetries
    )
  }
}