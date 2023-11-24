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
package io.lenses.streamreactor.connect.gcp.storage.config

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.gcp.storage.sink.config.GCPStorageSinkConfigDef
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.CollectionHasAsScala

class GCPConfigSettingsTest extends AnyFlatSpec with Matchers with LazyLogging {

  "GCPConfigSettings" should "ensure all sink keys are lower case" in {

    val configKeys =
      GCPStorageSinkConfigDef.config.configKeys().keySet().asScala

    configKeys.size shouldBe 21
    configKeys.foreach {
      k => k.toLowerCase should be(k)
    }
  }
}
