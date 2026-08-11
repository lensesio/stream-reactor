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

import io.lenses.streamreactor.connect.cloud.common.sink.config.kcqlprops.SinkPropsSchema
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class KeySuffixTest extends AnyFlatSpec with Matchers with EitherValues {

  "KeySuffix" should "return None if the KCQL properties does not contain key.suffix" in {
    KeySuffix.from(Map.empty, SinkPropsSchema.schema).value should be(None)
  }

  "KeySuffix" should "return the key.suffix value from the KCQL properties" in {
    KeySuffix.from(Map("key.suffix" -> "suffix"), SinkPropsSchema.schema).value should be(Some("suffix"))
  }

  "KeySuffix" should "leave an empty key.suffix unchanged" in {
    KeySuffix.from(Map("key.suffix" -> ""), SinkPropsSchema.schema).value should be(Some(""))
  }

  "KeySuffix" should "reject a key.suffix that begins with a digit because it is concatenated onto the timestamp field with no delimiter" in {
    KeySuffix.from(Map("key.suffix" -> "1foo"), SinkPropsSchema.schema).left.value.getMessage should
      (include("key.suffix") and include("digit"))
  }

  "KeySuffix" should "reject a single-digit key.suffix" in {
    KeySuffix.from(Map("key.suffix" -> "9"), SinkPropsSchema.schema).left.value.getMessage should
      (include("key.suffix") and include("digit"))
  }

  "KeySuffix" should "accept a key.suffix that does not begin with a digit" in {
    KeySuffix.from(Map("key.suffix" -> "foo1"), SinkPropsSchema.schema).value should be(Some("foo1"))
  }

}
