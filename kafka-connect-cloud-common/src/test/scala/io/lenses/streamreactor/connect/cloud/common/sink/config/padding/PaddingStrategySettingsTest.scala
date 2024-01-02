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
package io.lenses.streamreactor.connect.cloud.common.sink.config.padding

import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.cloud.common.sink.config.TestConfig
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.LeftPadPaddingStrategy
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.PaddingStrategyConfigKeys
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PaddingStrategySettingsTest extends AnyFunSuite with Matchers with PaddingStrategyConfigKeys {

  test("getPaddingStrategy should return None when padding strategy is not provided") {
    val settings = TestConfig()
    settings.getPaddingStrategy should equal(None)
  }

  test("getPaddingStrategy should return None when padding length is less than 0") {
    val settings = TestConfig(PADDING_STRATEGY -> "-1")
    settings.getPaddingStrategy should equal(None)
  }

  test("getPaddingStrategy should return None when padding length is much less than 0") {
    val settings = TestConfig(PADDING_LENGTH -> "-199")
    settings.getPaddingStrategy should equal(None)
  }

  test("getPaddingStrategy should return the appropriate PaddingStrategy") {
    val settings = TestConfig(
      PADDING_LENGTH   -> "12",
      PADDING_STRATEGY -> "LeftPad",
    )
    settings.getPaddingStrategy should equal(LeftPadPaddingStrategy(12, '0').some)
  }

  override def connectorPrefix: String = "connect.testing"
}
