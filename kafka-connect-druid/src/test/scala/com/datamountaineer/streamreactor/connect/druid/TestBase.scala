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

package com.datamountaineer.streamreactor.connect.druid

import java.nio.file.Paths
import java.util

import com.datamountaineer.streamreactor.connect.druid.config.{DruidSinkConfig, DruidSinkConfigConstants}

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 07/12/2016. 
  * stream-reactor
  */
trait TestBase {

  val DATA_SOURCE = "wikipedia"
  val TOPIC = "test"
  val KCQL = s"INSERT INTO $DATA_SOURCE SELECT page, robot AS bot, country FROM $TOPIC"
  lazy val DRUID_CONFIG_FILE: String = Paths.get(getClass.getResource("/ds-template.json").toURI).toAbsolutePath.toFile.getAbsolutePath

  def getProps(): util.Map[String, String] = {
    Map(DruidSinkConfigConstants.KCQL->KCQL, DruidSinkConfigConstants.CONFIG_FILE->DRUID_CONFIG_FILE).asJava
  }

  def getPropsNoFile(): util.Map[String, String] = {
    Map(DruidSinkConfigConstants.KCQL->KCQL).asJava
  }

  def getPropWrongPath(): util.Map[String, String] = {
    Map(DruidSinkConfigConstants.KCQL->KCQL, DruidSinkConfigConstants.CONFIG_FILE->"bah").asJava
  }

}
