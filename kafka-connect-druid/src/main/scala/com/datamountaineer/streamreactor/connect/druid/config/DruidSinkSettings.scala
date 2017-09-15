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

package com.datamountaineer.streamreactor.connect.druid.config

import java.io.File

import com.datamountaineer.streamreactor.connect.schemas.StructFieldsExtractor
import org.apache.kafka.common.config.ConfigException

import scala.collection.JavaConversions._

case class DruidSinkSettings(datasourceNames: Map[String,String],
                             tranquilityConfig: String,
                             extractors: Map[String, StructFieldsExtractor]
                            )

object DruidSinkSettings {
  /**
    * Creates an instance of DruidSinkSettings from a DruidSinkConfig
    *
    * @param config : The map of all provided configurations
    * @return An instance of DruidSinkSettings
    */
  def apply(config: DruidConfig): DruidSinkSettings = {
    val file = config.getString(DruidSinkConfigConstants.CONFIG_FILE)
    if (file.trim.length == 0 || !new File(file).exists()) {
      throw new ConfigException(s"${DruidSinkConfigConstants.CONFIG_FILE} is not set correctly.")
    }

    val fileContents = scala.io.Source.fromFile(file).mkString

    if (fileContents.isEmpty) {
      throw new ConfigException(s"Empty ${DruidSinkConfigConstants.CONFIG_FILE}.")
    }

    val kcql = config.getKCQL
    val dataSources = config.getTableTopic()
    val fields = config.getFieldsMap(kcql)

    val extractors = kcql.map(r => {
      val ignore = if (r.getIgnoredFields.nonEmpty) true else false
      (r.getSource, StructFieldsExtractor(ignore, fields(r.getSource)))
    }).toMap

    DruidSinkSettings(dataSources, scala.io.Source.fromFile(file).mkString, extractors)
  }
}
