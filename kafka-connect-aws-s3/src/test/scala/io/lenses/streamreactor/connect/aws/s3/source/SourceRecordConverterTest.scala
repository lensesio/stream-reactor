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

import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3RootLocation
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SourceRecordConverterTest extends AnyFlatSpec with Matchers {

  "fromSourcePartition" should "convert RemoteS3RootLocation to Map" in {
    SourceRecordConverter.fromSourcePartition(RemoteS3RootLocation("test-bucket:test-prefix")) should contain allOf (
      "container" -> "test-bucket",
      "prefix"    -> "test-prefix"
    )
  }

  "fromSourcePartition" should "convert RemoteS3RootLocation without prefix to Map" in {
    SourceRecordConverter.fromSourcePartition(RemoteS3RootLocation("test-bucket")) should contain allOf (
      "container" -> "test-bucket",
      "prefix"    -> ""
    )
  }

  "fromSourceOffset" should "convert RemoteS3RootLocation to Map" in {
    SourceRecordConverter.fromSourceOffset(
      RemoteS3RootLocation("test-bucket:test-prefix").withPath("test-path"),
      100L,
    ) should contain allOf (
      "path" -> "test-path",
      "line" -> "100"
    )
  }

}
