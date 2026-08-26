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
package io.lenses.streamreactor.connect.elastic.common.config

import org.apache.kafka.common.config.ConfigException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ElasticWriteTimeoutValidatorTest extends AnyFunSuite with Matchers {

  private val configKey = "connect.elastic.write.timeout"

  test("rejects values in the pre-migration seconds trap range") {
    val ex = intercept[ConfigException](ElasticWriteTimeoutValidator.validate(60, configKey))
    ex.getMessage should include(configKey)
    ex.getMessage should include("seconds")
  }

  test("allows sub-second timeouts outside the seconds trap range") {
    noException shouldBe thrownBy(ElasticWriteTimeoutValidator.validate(750, configKey))
  }

  test("allows values above the seconds trap range") {
    noException shouldBe thrownBy(ElasticWriteTimeoutValidator.validate(60000, configKey))
  }
}
