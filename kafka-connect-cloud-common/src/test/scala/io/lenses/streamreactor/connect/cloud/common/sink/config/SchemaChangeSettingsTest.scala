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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SchemaChangeSettingsTest extends AnyFlatSpec with Matchers {

  "withSchemaChangeConfig" should "define schema change detector config with default value" in {

    val configDef = new SchemaChangeConfigKeys {
      override def connectorPrefix: String = "connector"
    }.withSchemaChangeConfig(new ConfigDef())
    val configKey = configDef.configKeys().get("connector.schema.change.detector")

    configKey.defaultValue shouldBe "default"
    configKey.documentation shouldBe "Schema change detector."
  }

}
