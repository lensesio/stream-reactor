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
package io.lenses.streamreactor.connect.cloud.common.formats.reader

import cats.data.Validated
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

class EmptyCloudStreamReaderTest extends AnyFunSuiteLike with Matchers {

  test("EmptyCloudStreamReader should return false for hasNext") {
    val emptyCloudStreamReader = new EmptyCloudStreamReader(null)
    emptyCloudStreamReader.hasNext shouldBe false
  }

  test("EmptyCloudStreamReader should throw UnsupportedOperationException for next") {
    val emptyCloudStreamReader = new EmptyCloudStreamReader(null)
    assertThrows[UnsupportedOperationException] {
      emptyCloudStreamReader.next()
    }
  }

  test("EmptyCloudStreamReader should return -1 for currentRecordIndex") {
    val emptyCloudStreamReader = new EmptyCloudStreamReader(null)
    emptyCloudStreamReader.currentRecordIndex shouldBe -1
  }

  test("EmptyCloudStreamReader should return the location provided in the constructor") {
    implicit val validator: CloudLocationValidator = new CloudLocationValidator {
      override def validate(location: CloudLocation): Validated[Throwable, CloudLocation] = Validated.valid(location)
    }
    val location               = new CloudLocation("bucket", Some("path"))
    val emptyCloudStreamReader = new EmptyCloudStreamReader(location)
    emptyCloudStreamReader.getBucketAndPath shouldBe location
  }

  test("EmptyCloudStreamReader should return Unit for close") {
    val emptyCloudStreamReader = new EmptyCloudStreamReader(null)
    emptyCloudStreamReader.close() shouldBe ()
  }
}
