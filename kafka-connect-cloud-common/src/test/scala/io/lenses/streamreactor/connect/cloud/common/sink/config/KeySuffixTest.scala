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
package io.lenses.streamreactor.connect.cloud.common.sink.config

import io.lenses.streamreactor.connect.cloud.common.sink.config.kcqlprops.SinkPropsSchema
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class KeySuffixTest extends AnyFlatSpec with Matchers {

  "KeySuffix" should "return None if the KCQL properties does not contain key.prefix" in {
    KeySuffix.from(Map.empty, SinkPropsSchema.schema) should be(None)
  }

  "KeySuffix" should "return the key.prefix value from the KCQL properties" in {
    KeySuffix.from(Map("key.suffix" -> "suffix"), SinkPropsSchema.schema) should be(Some("suffix"))
  }

}
