/*
 * Copyright 2017-2023 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.cloud.common.sink.config

import com.datamountaineer.streamreactor.common.config.base.traits.BaseConfig
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.PaddingStrategyConfigKeys
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.PaddingStrategySettings
import io.lenses.streamreactor.connect.cloud.common.sink.config.LocalStagingAreaConfigKeys
import io.lenses.streamreactor.connect.cloud.common.sink.config.LocalStagingAreaSettings
import org.apache.kafka.common.config.ConfigDef

import java.util
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala

case class TestConfigDefBuilder(configDef: ConfigDef, props: util.Map[String, String])
    extends BaseConfig("connect.testing", configDef, props)
    with PaddingStrategySettings
    with LocalStagingAreaSettings {

  def getParsedValues: Map[String, _] = values().asScala.toMap

}

object TestConfig {

  def apply(pairs: (String, String)*): TestConfigDefBuilder = {

    def defineProps(configDef: ConfigDef) = {
      val keys = new PaddingStrategyConfigKeys with LocalStagingAreaConfigKeys {
        override def connectorPrefix: String = "connect.testing"
      }
      keys.addPaddingToConfigDef(configDef)
      keys.addLocalStagingAreaToConfigDef(configDef)
    }

    val map: Map[String, String] = pairs.toMap
    val newMap = map + {
      "connect.s3.kcql" -> "dummy value"
    }
    TestConfigDefBuilder(defineProps(new ConfigDef()), newMap.asJava)
  }

}
