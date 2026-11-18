/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.config

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.types.Password

import java.util
import scala.jdk.CollectionConverters._

/** Parses index properties through the real `ConfigDef` into an `IndexSettings`, as a sink task does. */
object IndexSettingsFixture {

  val prefix = "connect.testcloud"

  object Keys extends IndexConfigKeys {
    override def connectorPrefix: String = prefix
  }

  def configDef: ConfigDef = Keys.addIndexSettingsToConfigDef(new ConfigDef())

  def settingsFor(props: Map[String, String]): IndexSettings = {
    val parsed = new AbstractConfig(configDef, props.asJava)
    new IndexSettings {
      override def connectorPrefix: String = prefix
      override def getString(key:   String): String            = parsed.getString(key)
      override def getInt(key:      String): Integer           = parsed.getInt(key)
      override def getLong(key:     String): java.lang.Long    = parsed.getLong(key)
      override def getBoolean(key:  String): java.lang.Boolean = parsed.getBoolean(key)
      override def getPassword(key: String): Password          = parsed.getPassword(key)
      override def getList(key:     String): util.List[String] = parsed.getList(key)
    }
  }
}
