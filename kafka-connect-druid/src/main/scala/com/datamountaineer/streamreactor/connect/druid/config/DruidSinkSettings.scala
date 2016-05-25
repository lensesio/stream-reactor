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

package com.datamountaineer.streamreactor.connect.druid.config

import java.io.File
import com.datamountaineer.streamreactor.connect.druid.config.DruidSinkConfig._
import com.datamountaineer.streamreactor.connect.schemas.PayloadFields
import io.confluent.common.config.ConfigException
import scala.util.Try

case class DruidSinkSettings(datasourceName: String,
                             tranquilityConfig: String,
                             payloadFields: PayloadFields
                            )

object DruidSinkSettings {
  /**
    * Creates an instance of DruidSinkSettings from a DruidSinkConfig
    *
    * @param config : The map of all provided configurations
    * @return An instance of DruidSinkSettings
    */
  def apply(config: DruidSinkConfig): DruidSinkSettings = {
    val dataSource = config.getString(DATASOURCE_NAME)
    if (dataSource.trim.length == 0) {
      throw new ConfigException(s"$DATASOURCE_NAME is not set up correctly.")
    }
    val file = config.getString(CONFIG_FILE)
    if (file.trim.length == 0 || !new File(file).exists()) {
      throw new ConfigException(s"$CONFIG_FILE is not set correctly.")
    }

    //val timeout = Try(config.getInt(TIMEOUT)).toOption.getOrElse(600)


    DruidSinkSettings(
      dataSource,
      scala.io.Source.fromFile(file).mkString,
      PayloadFields(Try(config.getString(FIELDS)).toOption.flatMap(v => Option(v)))
    )
  }
}
