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
package io.lenses.streamreactor.connect.aws.s3.source.files

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.aws.s3.config.Format
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3RootLocation
import io.lenses.streamreactor.connect.aws.s3.storage.SourceStorageInterface
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class S3SourceListerTest extends AnyFlatSpec with MockitoSugar with Matchers {

  private implicit val storageInterface: SourceStorageInterface = mock[SourceStorageInterface]

  private val bucketAndPrefix = RemoteS3RootLocation("my-bucket:path")
  private val sourceLister    = new S3SourceLister(Format.Json)

  "listBatch" should "return first result when no TopicPartitionOffset has been provided" in {

    when(storageInterface.list(bucketAndPrefix, None, 10)).thenReturn(
      List("path/myTopic/0/100.json", "path/myTopic/0/200.json", "path/myTopic/0/300.json")
        .asRight,
    )

    sourceLister.listBatch(bucketAndPrefix, None, 10) should be(
      Right(
        List(
          bucketAndPrefix.withPath("path/myTopic/0/100.json"),
          bucketAndPrefix.withPath("path/myTopic/0/200.json"),
          bucketAndPrefix.withPath("path/myTopic/0/300.json"),
        ),
      ),
    )
  }

  "listBatch" should "return empty when no results are found" in {

    when(storageInterface.list(bucketAndPrefix, None, 10)).thenReturn(
      List.empty.asRight,
    )

    sourceLister.listBatch(bucketAndPrefix, None, 10) should be(
      Right(List()),
    )
  }

  "listBatch" should "ignore other, unknown extensions and files without extensions" in {
    when(storageInterface.list(bucketAndPrefix, None, 10)).thenReturn(
      List(
        "path/myTopic/0/100.json",
        "path/myTopic/0/200.xls",
        "path/myTopic/0/300.doc",
        "path/myTopic/0/400.csv",
        "path/myTopic/0/500",
      )
        .asRight,
    )

    sourceLister.listBatch(bucketAndPrefix, None, 10) should be(
      Right(
        List(
          bucketAndPrefix.withPath("path/myTopic/0/100.json"),
        ),
      ),
    )
  }

  "listBatch" should "return throwable when storageInterface errors" in {
    val exception = new IllegalStateException("BadThingsHappened")

    when(storageInterface.list(bucketAndPrefix, None, 10)).thenReturn(
      exception.asLeft,
    )

    sourceLister.listBatch(bucketAndPrefix, None, 10) should be(
      Left(exception),
    )
  }

}
