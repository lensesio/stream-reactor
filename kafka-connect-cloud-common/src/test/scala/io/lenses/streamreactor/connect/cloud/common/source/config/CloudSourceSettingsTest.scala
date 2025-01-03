/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.source.config

import org.apache.kafka.common.config.types.Password
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.lang
import java.util

class CloudSourceSettingsTest extends AnyFlatSpec with Matchers with OptionValues {

  "getSourceExtensionFilter" should "return an ExtensionFilter with correct includes and excludes" in {
    val settings: CloudSourceSettings = mockSettingsObject(
      includes = ".txt,.csv",
      excludes = ".log",
    )

    val filter = settings.getSourceExtensionFilter.value
    filter.allowedExtensions should be(Set(".txt", ".csv"))
    filter.excludedExtensions should be(Set(".log"))
  }

  "getSourceExtensionFilter" should "return an ExtensionFilter with correct includes and if no dots used" in {
    val settings: CloudSourceSettings = mockSettingsObject(
      includes = "txt",
      excludes = "log,csv",
    )

    val filter = settings.getSourceExtensionFilter.value
    filter.allowedExtensions should be(Set(".txt"))
    filter.excludedExtensions should be(Set(".log", ".csv"))
  }

  private def mockSettingsObject(includes: String, excludes: String) = new CloudSourceSettings {
    override def getString(key: String): String = key match {
      case SOURCE_EXTENSION_INCLUDES => includes
      case SOURCE_EXTENSION_EXCLUDES => excludes
      case _                         => ""
    }

    override def getInt(key: String): Integer = ???

    override def getLong(key: String): lang.Long = ???

    override def getBoolean(key: String): lang.Boolean = ???

    override def getPassword(key: String): Password = ???

    override def getList(key: String): util.List[String] = ???

    override def connectorPrefix: String = "my.connector"
  }
}
