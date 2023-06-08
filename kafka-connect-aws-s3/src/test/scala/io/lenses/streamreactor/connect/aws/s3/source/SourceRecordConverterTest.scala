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
package io.lenses.streamreactor.connect.aws.s3.source

import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class SourceRecordConverterTest extends AnyFlatSpec with Matchers {

  "fromSourcePartition" should "convert S3Location to Map" in {
    SourceRecordConverter.fromSourcePartition(S3Location("test-bucket", "test-prefix".some)) should contain allOf (
      "container" -> "test-bucket",
      "prefix"    -> "test-prefix"
    )
  }

  "fromSourcePartition" should "convert S3Location without prefix to Map" in {
    SourceRecordConverter.fromSourcePartition(S3Location("test-bucket")) should contain allOf (
      "container" -> "test-bucket",
      "prefix"    -> ""
    )
  }

  "fromSourceOffset" should "convert S3Location to Map" in {
    val nowInst = Instant.now
    SourceRecordConverter.fromSourceOffset(
      S3Location("test-bucket", "test-prefix".some).withPath("test-path"),
      100L,
      nowInst,
    ) should contain allOf (
      "path" -> "test-path",
      "line" -> "100",
      "ts"   -> nowInst.toEpochMilli.toString,
    )
  }

}
