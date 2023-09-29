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
package io.lenses.streamreactor.connect.aws.s3.sink

import io.lenses.streamreactor.connect.cloud.sink.config.padding.LeftPadPaddingStrategy
import io.lenses.streamreactor.connect.cloud.sink.config.padding.RightPadPaddingStrategy
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class PaddingStrategyTest extends AnyFlatSpecLike with Matchers {

  "LeftPaddingStrategy" should "pad string left" in {
    LeftPadPaddingStrategy(5, '0').padString("2") should be("00002")
  }

  "RightPaddingStrategy" should "pad string right" in {
    RightPadPaddingStrategy(10, '0').padString("3") should be("3000000000")
  }
}
