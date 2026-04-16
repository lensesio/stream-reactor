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

import org.apache.kafka.connect.header.ConnectHeaders
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

class HeaderTest extends AnyFunSuiteLike with Matchers {

  test("Header.get should return the header value when the locator is valid") {
    val headers    = new ConnectHeaders().add("test-header", "test-value", null)
    val sinkRecord = new SinkRecord("topic", 0, null, null, null, null, 0, null, null, headers)
    val result     = Header.get(Some("test-header"), sinkRecord)
    result shouldBe Right("test-value")
  }

  test("Header.get should return an error when the header is not found") {
    val headers    = new ConnectHeaders()
    val sinkRecord = new SinkRecord("topic", 0, null, null, null, null, 0, null, null, headers)
    val result     = Header.get(Some("non-existent-header"), sinkRecord)
    result shouldBe Left(SubstitutionError("Header with name `non-existent-header` not found"))
  }

  test("Header.get should return an error when the header value is null") {
    val headers    = new ConnectHeaders().add("test-header", null, null)
    val sinkRecord = new SinkRecord("topic", 0, null, null, null, null, 0, null, null, headers)
    val result     = Header.get(Some("test-header"), sinkRecord)
    result shouldBe Left(SubstitutionError("Header value for `test-header` is null"))
  }

  test("Header.get should return an error when the locator is None") {
    val headers    = new ConnectHeaders()
    val sinkRecord = new SinkRecord("topic", 0, null, null, null, null, 0, null, null, headers)
    val result     = Header.get(None, sinkRecord)
    result shouldBe Left(SubstitutionError("No header specified for substitution"))
  }
}
