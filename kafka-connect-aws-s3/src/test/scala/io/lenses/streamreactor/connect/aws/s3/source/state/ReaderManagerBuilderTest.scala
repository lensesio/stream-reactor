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
package io.lenses.streamreactor.connect.aws.s3.source.state

import cats.effect.testing.scalatest.AsyncIOSpec
import io.lenses.streamreactor.connect.aws.s3.config.AvroFormatSelection
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.source.config.OrderingType
import io.lenses.streamreactor.connect.aws.s3.source.config.SourceBucketOptions
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import org.mockito.MockitoSugar.mock
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class ReaderManagerBuilderTest extends AsyncFlatSpec with AsyncIOSpec with Matchers {
  "ReaderManagerBuilder" should "create a reader manager" in {
    val si = mock[StorageInterface]
    val root = S3Location(
      "bucket",
      Some("prefix1"),
      None,
      None,
      None,
    )
    val path = "prefix1/subprefixA/subprefixB/"
    var rootValue: Option[S3Location] = None
    val contextF: S3Location => Option[S3Location] = { in =>
      rootValue = Some(in)
      rootValue
    }
    val sbo    = SourceBucketOptions(root, "topic", AvroFormatSelection, 100, 100, None, OrderingType.LastModified, false)
    val taskId = ConnectorTaskId("test", 3, 1)
    ReaderManagerBuilder(root, path, si, taskId, contextF, _ => Some(sbo))
      .asserting(_ => rootValue shouldBe Some(root.copy(prefix = Some(path))))
  }

}
