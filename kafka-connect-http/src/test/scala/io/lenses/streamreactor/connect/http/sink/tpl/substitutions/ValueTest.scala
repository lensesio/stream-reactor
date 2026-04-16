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
package io.lenses.streamreactor.connect.http.sink.tpl.substitutions

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

class ValueTest extends AnyFunSuiteLike with Matchers {

  test("Value.get should return the extracted value when the locator is valid") {
    val sinkRecord = new SinkRecord("topic", 0, null, null, Schema.STRING_SCHEMA, "test-value", 0)
    val result     = Value.get(Some("valid-locator"), sinkRecord)
    result shouldBe Right("test-value")
  }

}
