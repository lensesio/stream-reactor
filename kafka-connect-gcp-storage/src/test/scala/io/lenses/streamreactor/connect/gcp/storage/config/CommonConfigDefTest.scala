/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.gcp.storage.config

import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.gcp.common.config.AuthModeSettings
import io.lenses.streamreactor.connect.gcp.common.config.GCPSettings
import io.lenses.streamreactor.connect.gcp.storage.config.GCPConfigSettings.CONNECTOR_PREFIX
import io.lenses.streamreactor.connect.gcp.storage.config.GCPConfigSettings.KCQL_CONFIG
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala

class CommonConfigDefTest extends AnyFlatSpec with Matchers with EitherValues with UploadConfigKeys {

  private val authModeConfig = new AuthModeSettings(javaConnectorPrefix)
  private val gcpSettings    = new GCPSettings(javaConnectorPrefix)

  private val commonConfigDef = new CommonConfigDef {
    override def connectorPrefix: String = CONNECTOR_PREFIX
  }

  private val DefaultProps: Map[String, String] =
    Map(
      gcpSettings.getGcpProjectId   -> "projectId",
      authModeConfig.getAuthModeKey -> "none",
      gcpSettings.getHost           -> "localhost:9090",
      KCQL_CONFIG                   -> "SELECT * FROM DEFAULT",
    )

  "CommonConfigDef" should "retain original properties after parsing" in {
    val resultMap = commonConfigDef.config.parse(DefaultProps.asJava).asScala
    resultMap should have size 15
    DefaultProps.foreach {
      case (k, v) =>
        withClue("Unexpected property: " + k) {
          resultMap.get(k) should be(v.some)
        }
    }
  }

  override def connectorPrefix: String = CONNECTOR_PREFIX
}
