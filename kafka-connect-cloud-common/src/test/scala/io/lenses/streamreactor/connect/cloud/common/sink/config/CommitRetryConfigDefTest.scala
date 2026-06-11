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
package io.lenses.streamreactor.connect.cloud.common.sink.config

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.MapHasAsJava

class CommitRetryConfigDefTest extends AnyFlatSpec with Matchers {

  private val prefix = "connect.test"

  private val configKeys = new CommitRetryConfigKeys {
    override def connectorPrefix: String = prefix
  }

  private val configDef: ConfigDef = configKeys.addCommitRetrySettingsToConfigDef(new ConfigDef())

  private def minimalValidProps: Map[String, String] = Map(
    configKeys.COMMIT_RETRY_MAX_ATTEMPTS -> "5",
    configKeys.COMMIT_RETRY_BASE_DELAY_MS -> "200",
    configKeys.COMMIT_RETRY_MULTIPLIER -> "2.0",
    configKeys.COMMIT_RETRY_MAX_DELAY_MS -> "5000",
  )

  "CommitRetryConfigDef" should "parse valid defaults without error" in {
    noException should be thrownBy configDef.parse(minimalValidProps.asJava)
  }

  it should "accept multiplier equal to 1.0 (constant interval)" in {
    noException should be thrownBy configDef.parse(minimalValidProps.updated(configKeys.COMMIT_RETRY_MULTIPLIER, "1.0").asJava)
  }

  it should "accept multiplier of 1.5" in {
    noException should be thrownBy configDef.parse(minimalValidProps.updated(configKeys.COMMIT_RETRY_MULTIPLIER, "1.5").asJava)
  }

  it should "reject multiplier below 1.0" in {
    a[ConfigException] should be thrownBy configDef.parse(
      minimalValidProps.updated(configKeys.COMMIT_RETRY_MULTIPLIER, "0.9").asJava,
    )
  }

  it should "reject multiplier of zero" in {
    a[ConfigException] should be thrownBy configDef.parse(
      minimalValidProps.updated(configKeys.COMMIT_RETRY_MULTIPLIER, "0.0").asJava,
    )
  }

  it should "reject a negative multiplier" in {
    a[ConfigException] should be thrownBy configDef.parse(
      minimalValidProps.updated(configKeys.COMMIT_RETRY_MULTIPLIER, "-1.0").asJava,
    )
  }
}
