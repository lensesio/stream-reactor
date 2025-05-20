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
package io.lenses.streamreactor.connect.cloud.common.source

import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.jdk.CollectionConverters.MapHasAsScala

class SourceWatermarkTest extends AnyFlatSpec with Matchers {
  private implicit val cloudLocationValidator: CloudLocationValidator = SampleData.cloudLocationValidator

  "fromSourcePartition" should "convert S3Location to Map" in {
    SourceWatermark.partition(CloudLocation("test-bucket", "test-prefix".some)).asScala.toMap shouldBe Map(
      "container" -> "test-bucket",
      "prefix"    -> "test-prefix",
    )
  }

  "fromSourcePartition" should "convert S3Location without prefix to Map" in {
    SourceWatermark.partition(CloudLocation("test-bucket")).asScala.toMap shouldBe Map(
      "container" -> "test-bucket",
      "prefix"    -> "",
    )
  }

  "fromSourceOffset" should "convert S3Location to Map" in {
    val nowInst = Instant.now
    SourceWatermark.offset(
      CloudLocation("test-bucket", "test-prefix".some).withPath("test-path"),
      100L,
      nowInst,
      lastLine = true,
    ).asScala.toMap shouldBe Map(
      "path" -> "test-path",
      "line" -> "100",
      "ts"   -> nowInst.toEpochMilli.toString,
      "last" -> "t",
    )
  }

}
