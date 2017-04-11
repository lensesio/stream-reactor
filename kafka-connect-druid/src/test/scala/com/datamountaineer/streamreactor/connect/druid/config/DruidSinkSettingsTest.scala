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

import java.nio.file.Paths

import com.datamountaineer.streamreactor.connect.druid.TestBase
import com.datamountaineer.streamreactor.connect.schemas.StructFieldsExtractor
import org.apache.kafka.common.config.ConfigException
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mock.MockitoSugar

class DruidSinkSettingsTest extends WordSpec with TestBase with Matchers with MockitoSugar {
  "DruidSinkSettings" should {
    "raise an exception if the config file is not specified" in {
      intercept[ConfigException] {
        val config = new DruidSinkConfig(getPropsNoFile())
        DruidSinkSettings(config)
      }
    }
    "raise an exception if the config file specified doesn't exist" in {
      intercept[ConfigException] {
        val config = new DruidSinkConfig(getPropWrongPath())
        DruidSinkSettings(config)
      }
    }
    "create an instance of DruidSinkSettings with field alias" in {
      val config = mock[DruidSinkConfig]
      when(config.getString(DruidSinkConfigConstants.KCQL)).thenReturn(KCQL)
      when(config.getString(DruidSinkConfigConstants.CONFIG_FILE)).thenReturn(Paths.get(getClass.getResource(s"/ds-template.json").toURI).toAbsolutePath.toString)
      val settings = DruidSinkSettings(config)

      settings.datasourceNames shouldBe Map(TOPIC->DATA_SOURCE)
      settings.tranquilityConfig shouldBe scala.io.Source.fromFile(Paths.get(getClass.getResource(s"/ds-template.json").toURI).toFile).mkString

      val x: StructFieldsExtractor = settings.extractors.get(TOPIC).get
      x.includeAllFields shouldBe false
      x.fieldsAliasMap.get("page").get shouldBe "page"
      x.fieldsAliasMap.get("robot").get shouldBe "bot"
      x.fieldsAliasMap.get("country").get shouldBe "country"
    }

  }
}
