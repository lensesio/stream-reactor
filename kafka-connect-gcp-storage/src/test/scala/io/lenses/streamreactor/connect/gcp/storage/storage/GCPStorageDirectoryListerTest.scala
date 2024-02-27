/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.gcp.storage.storage

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import com.google.api.gax.paging.Page
import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.Bucket
import com.google.cloud.storage.Storage
import com.google.cloud.storage.Storage.BlobListOption
import com.google.cloud.storage.Storage.BucketGetOption
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.gcp.storage.model.location.GCPStorageLocationValidator
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters.IterableHasAsJava

class GCPStorageDirectoryListerTest
    extends AsyncFlatSpecLike
    with AsyncIOSpec
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar {
  private implicit val cloudLocationValidator: CloudLocationValidator = GCPStorageLocationValidator

  private val connectorTaskId: ConnectorTaskId = ConnectorTaskId("sinkName", 1, 1)

  private val bucketName = "bucket"

  "lister" should "list all directories" in {

    val mockClient: Storage = setUpMockClient(Seq(
      "prefix1/",
      "prefix2/",
      "prefix3/",
      "prefix4/",
    ))

    check(
      mockClient,
      connectorTaskId,
      CloudLocation("bucket", none),
      0,
      Set.empty,
      Set.empty,
      Set("prefix1/", "prefix2/", "prefix3/", "prefix4/"),
    )
  }

  "lister" should "list directories recursively" in {

    val mockClient: Storage = setUpMockClient(
      Seq(
        "prefix1/",
        "prefix2/",
        "prefix3/",
        "prefix4/",
      ),
      Seq("prefix1/sub1/"),
      Seq("prefix2/sub2/"),
      Seq("prefix3/sub3/"),
      Seq("prefix4/sub4/"),
    )

    check(
      mockClient,
      connectorTaskId,
      CloudLocation("bucket", none),
      2,
      Set.empty,
      Set.empty,
      Set("prefix1/sub1/", "prefix2/sub2/", "prefix3/sub3/", "prefix4/sub4/"),
    )
  }

  "lister" should "exclude directories" in {

    val mockClient: Storage = setUpMockClient(
      Seq(
        "prefix1/",
        "prefix2/",
        "prefix3/",
        "prefix4/",
      ),
    )

    check(
      mockClient,
      location         = CloudLocation("bucket", none),
      recursiveLevel   = 0,
      exclude          = Set("prefix1/", "prefix4/"),
      wildcardExcludes = Set.empty,
      expected         = Set("prefix2/", "prefix3/"),
    )
  }

  "lister" should "consider the connector task owning the partition" in {
    val taskId1 = ConnectorTaskId("sinkName", 2, 1)
    val taskId2 = ConnectorTaskId("sinkName", 2, 0)

    val mockClient: Storage = setUpMockClient(Seq(
      "prefix1/",
      "prefix2/",
      "prefix3/",
      "prefix4/",
    ))

    check(mockClient, taskId1, CloudLocation("bucket", none), 0, Set.empty, Set.empty, Set("prefix2/", "prefix4/"))

    check(mockClient, taskId2, CloudLocation("bucket", none), 0, Set.empty, Set.empty, Set("prefix1/", "prefix3/"))

    check(mockClient, taskId1, CloudLocation("bucket", none), 0, Set("prefix2/", "prefix4/"), Set.empty, Set.empty)

    check(mockClient, taskId2, CloudLocation("bucket", none), 0, Set("prefix1/", "prefix3/"), Set.empty, Set.empty)

    check(mockClient, taskId1, CloudLocation("bucket", none), 0, Set("prefix2/"), Set.empty, Set("prefix4/"))

    check(mockClient, taskId2, CloudLocation("bucket", none), 0, Set("prefix1/"), Set.empty, Set("prefix3/"))
  }

  "lister" should "exclude indexes directory when configured as wildcard exclude" in {

    val mockClient: Storage = setUpMockClient(
      Seq(
        ".indexes/sinkName/myTopic/00005/00000000000000000050",
        ".indexes/sinkName/myTopic/00005/00000000000000000070",
        ".indexes/sinkName/myTopic/00005/00000000000000000100",
        "prefix1/",
        "prefix1/",
        "prefix2/",
        "prefix2/",
      ),
    )

    check(
      mockClient,
      location         = CloudLocation("bucket", "prefix1/".some),
      exclude          = Set.empty,
      wildcardExcludes = Set(".indexes"),
      recursiveLevel   = 0,
      expected         = Set("prefix1/"),
    )
    check(
      mockClient,
      location         = CloudLocation("bucket", "prefix2/".some),
      exclude          = Set.empty,
      wildcardExcludes = Set(".indexes"),
      recursiveLevel   = 0,
      expected         = Set("prefix2/"),
    )
    check(
      mockClient,
      location         = CloudLocation("bucket", "prefix3/".some),
      exclude          = Set.empty,
      wildcardExcludes = Set(".indexes"),
      recursiveLevel   = 0,
      expected         = Set.empty,
    )
    check(
      mockClient,
      location         = CloudLocation("bucket", None),
      exclude          = Set.empty,
      wildcardExcludes = Set(".indexes"),
      recursiveLevel   = 0,
      expected         = Set("prefix1/", "prefix2/"),
    )
  }

  private def setUpMockClient(dirsToFind: Seq[String]*) = {
    val mockBucket = mock[Bucket]

    val mockBlobPages = dirsToFind.map {
      dirs =>
        val mockBlobPage: Page[Blob] = mockTheBlobPage(dirs)

        mockBlobPage
    }

    when(mockBucket.list(any[BlobListOption])) thenReturn (mockBlobPages.head, mockBlobPages.tail: _*)

    val mockClient = mock[Storage]
    when(
      mockClient.get(
        any[String],
        any[BucketGetOption],
      ),
    ).thenReturn(mockBucket)
    mockClient
  }

  private def mockTheBlobPage(dirsToFind: Seq[String]) = {
    val blobberable: Iterable[Blob] = dirsToFind.map { b =>
      val blobId: BlobId = BlobId.of(bucketName, b)

      val blob: Blob = mock[Blob]
      when(blob.getName).thenReturn(b)
      when(blob.getBlobId).thenReturn(blobId)
      when(blob.isDirectory).thenReturn(true)
      blob
    }

    val mockBlobPage = mock[Page[Blob]]
    when(mockBlobPage.iterateAll())
      .thenReturn(blobberable.asJava)
    mockBlobPage
  }

  private def check(
    client:           Storage,
    connectorTaskId:  ConnectorTaskId = connectorTaskId,
    location:         CloudLocation,
    recursiveLevel:   Int,
    exclude:          Set[String],
    wildcardExcludes: Set[String],
    expected:         Set[String],
  ) =
    new GCPStorageDirectoryLister(connectorTaskId, client).findDirectories(
      location,
      recursiveLevel,
      exclude,
      wildcardExcludes,
    ).asserting(actual => actual should be(expected))

}
