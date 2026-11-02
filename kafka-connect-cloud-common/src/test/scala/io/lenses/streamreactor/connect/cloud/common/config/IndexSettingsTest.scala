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

import io.lenses.streamreactor.connect.cloud.common.sink.seek.CommitMode
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.types.Password
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util
import scala.jdk.CollectionConverters._

class IndexSettingsTest extends AnyFunSuite with Matchers with OptionValues {

  private val prefix = "connect.testcloud"

  private object Keys extends IndexConfigKeys {
    override def connectorPrefix: String = prefix
  }

  private def configDef: ConfigDef = Keys.addIndexSettingsToConfigDef(new ConfigDef())

  private def settingsFor(props: Map[String, String]): IndexSettings = {
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

  test("[B] an absent commit.mode key yields CommitMode.Granular") {
    settingsFor(Map.empty).getIndexSettings.value.commitMode shouldBe CommitMode.Granular
  }

  test("[B] commit.mode=batch yields CommitMode.Batch") {
    settingsFor(Map(Keys.EXACTLY_ONCE_COMMIT_MODE -> "batch"))
      .getIndexSettings.value.commitMode shouldBe CommitMode.Batch
  }

  test("[B] an unrecognised commit.mode is rejected by ConfigDef validation") {
    val ex = intercept[ConfigException] {
      new AbstractConfig(configDef, Map(Keys.EXACTLY_ONCE_COMMIT_MODE -> "bogus").asJava)
    }
    ex.getMessage should include("bogus")
  }

  // ── case-insensitive commit.mode ────────────────────────────────────────────────────

  test("[B] commit.mode is accepted case-insensitively at the ConfigDef level") {
    Seq("batch", "Batch", "BATCH").foreach { value =>
      settingsFor(Map(Keys.EXACTLY_ONCE_COMMIT_MODE -> value))
        .getIndexSettings.value.commitMode shouldBe CommitMode.Batch
    }
    Seq("granular", "Granular", "GRANULAR").foreach { value =>
      settingsFor(Map(Keys.EXACTLY_ONCE_COMMIT_MODE -> value))
        .getIndexSettings.value.commitMode shouldBe CommitMode.Granular
    }
  }

  // ── commit.mode ignored when exactly-once disabled ──────────────────────────────────

  test("[B] commit.mode=batch has no effect (indexOptions is None) when exactly-once is disabled") {
    settingsFor(
      Map(
        Keys.ENABLE_EXACTLY_ONCE      -> "false",
        Keys.EXACTLY_ONCE_COMMIT_MODE -> "batch",
      ),
    ).getIndexSettings shouldBe None
  }
}
