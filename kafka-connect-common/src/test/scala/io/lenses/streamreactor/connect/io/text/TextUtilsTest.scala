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
package io.lenses.streamreactor.connect.io.text

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TextUtilsTest extends AnyFunSuite with Matchers {
  test("partiallyEndsWith") {
    TextUtils.partiallyEndsWith("abcde", "cde") should be(Some("cde"))
    TextUtils.partiallyEndsWith("abc", "bcde") should be(Some("bc"))
    TextUtils.partiallyEndsWith("aaabc", "bc") should be(Some("bc"))
    TextUtils.partiallyEndsWith("aaa\n b", "bc") should be(Some("b"))
  }
  test("partiallyEndsWith handles empty target") {
    TextUtils.partiallyEndsWith("", "cde") should be(None)
  }
  test("partiallyEndsWith handles empty source") {
    TextUtils.partiallyEndsWith("abcde", "") should be(None)
  }
}
