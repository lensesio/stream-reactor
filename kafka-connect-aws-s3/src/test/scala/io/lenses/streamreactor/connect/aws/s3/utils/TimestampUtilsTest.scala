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
package io.lenses.streamreactor.connect.aws.s3.utils

import io.lenses.streamreactor.connect.cloud.common.utils.TimestampUtils
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class TimestampUtilsTest extends AnyFunSuite with Matchers {
  test("test parseTime") {
    TimestampUtils.parseTime(Some(1609459200000L))(_ => fail("Should not be called")) match {
      case Some(value) =>
        value.compareTo(Instant.from(java.time.ZonedDateTime.parse("2021-01-01T00:00:00Z"))) shouldBe 0
      case None => fail("Should not be None")
    }
  }
  test("test parseTime with None as input") {
    TimestampUtils.parseTime(None)(_ => fail("Should not be called")) shouldBe None
  }

}
