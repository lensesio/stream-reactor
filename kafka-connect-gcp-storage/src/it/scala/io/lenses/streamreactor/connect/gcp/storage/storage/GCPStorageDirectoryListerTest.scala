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

import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import com.google.cloud.storage._
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.gcp.storage.model.location.GCPStorageLocationValidator
import io.lenses.streamreactor.connect.gcp.storage.utils.GCPProxyContainerTest
import org.scalatest.matchers.should.Matchers

class GCPStorageDirectoryListerTest extends GCPProxyContainerTest with Matchers {
  private implicit val cloudLocationValidator: CloudLocationValidator = GCPStorageLocationValidator

  private val filesLimit = 4

  "lister" should "list all directories" in {
    val bucketName = "listAllDirectories"
    val mockClient: Storage =
      setUpMockClient(bucketName, "prefix1/1.txt", "prefix2/2.txt", "prefix3/3.txt", "prefix4/4.txt")

    check(
      client           = mockClient,
      connectorTaskId  = connectorTaskId,
      location         = CloudLocation(bucketName, none),
      recursiveLevel   = 1,
      exclude          = Set.empty,
      wildcardExcludes = Set.empty,
      expected         = Set("prefix1/", "prefix2/", "prefix3/", "prefix4/"),
    )
  }

  "lister" should "list directories recursively" in {

    val bucketName = "listDirectoriesRecursively"
    val mockClient: Storage = setUpMockClient(
      bucketName,
      "prefix1/sub1/file1.txt",
      "prefix2/sub2/file2.txt",
      "prefix3/sub3/file3.txt",
      "prefix4/sub4/file4.txt",
    )

    check(
      client           = mockClient,
      connectorTaskId  = connectorTaskId,
      location         = CloudLocation(bucketName, none),
      recursiveLevel   = 2,
      exclude          = Set.empty,
      wildcardExcludes = Set.empty,
      expected         = Set("prefix1/sub1/", "prefix2/sub2/", "prefix3/sub3/", "prefix4/sub4/"),
    )
  }

  "lister" should "exclude directories" in {

    val bucketName = "excludeDirectories"
    val mockClient: Storage = setUpMockClient(
      bucketName,
      "prefix1/file1.txt",
      "prefix2/file2.txt",
      "prefix3/file3.txt",
      "prefix4/file4.txt",
    )

    check(
      client           = mockClient,
      location         = CloudLocation(bucketName, none),
      recursiveLevel   = 1,
      exclude          = Set("prefix1/", "prefix4/"),
      wildcardExcludes = Set.empty,
      expected         = Set("prefix2/", "prefix3/"),
    )
  }

  "lister" should "consider the connector task owning the partition" in {
    val bucketName = "connectorTaskIdOwnership"
    val taskId1    = ConnectorTaskId("sinkName", 2, 1)
    val taskId2    = ConnectorTaskId("sinkName", 2, 0)

    val mockClient: Storage = setUpMockClient(
      bucketName,
      "prefix1/file1.txt",
      "prefix2/file2.txt",
      "prefix3/file3.txt",
      "prefix4/file4.txt",
    )

    check(
      client           = mockClient,
      connectorTaskId  = taskId1,
      location         = CloudLocation(bucketName, none),
      recursiveLevel   = 1,
      exclude          = Set.empty,
      wildcardExcludes = Set.empty,
      expected         = Set("prefix2/", "prefix4/"),
    )

    check(
      client           = mockClient,
      connectorTaskId  = taskId2,
      location         = CloudLocation(bucketName, none),
      recursiveLevel   = 1,
      exclude          = Set.empty,
      wildcardExcludes = Set.empty,
      expected         = Set("prefix1/", "prefix3/"),
    )

    check(
      client           = mockClient,
      connectorTaskId  = taskId1,
      location         = CloudLocation(bucketName, none),
      recursiveLevel   = 1,
      exclude          = Set("prefix2/", "prefix4/"),
      wildcardExcludes = Set.empty,
      expected         = Set.empty,
    )

    check(
      client           = mockClient,
      connectorTaskId  = taskId2,
      location         = CloudLocation(bucketName, none),
      recursiveLevel   = 1,
      exclude          = Set("prefix1/", "prefix3/"),
      wildcardExcludes = Set.empty,
      expected         = Set.empty,
    )

    check(
      client           = mockClient,
      connectorTaskId  = taskId1,
      location         = CloudLocation(bucketName, none),
      recursiveLevel   = 1,
      exclude          = Set("prefix2/"),
      wildcardExcludes = Set.empty,
      expected         = Set("prefix4/"),
    )

    check(
      client           = mockClient,
      connectorTaskId  = taskId2,
      location         = CloudLocation(bucketName, none),
      recursiveLevel   = 1,
      exclude          = Set("prefix1/"),
      wildcardExcludes = Set.empty,
      expected         = Set("prefix3/"),
    )
  }

  "lister" should "exclude indexes directory when configured as wildcard exclude" in {

    val bucketName = "excludeIndexesDirectory"
    val mockClient: Storage = setUpMockClient(
      bucketName,
      ".indexes/sinkName/myTopic/00005/00000000000000000050",
      ".indexes/sinkName/myTopic/00005/00000000000000000070",
      ".indexes/sinkName/myTopic/00005/00000000000000000100",
      "prefix1/1.txt",
      "prefix1/2.txt",
      "prefix2/3.txt",
      "prefix2/4.txt",
    )

    check(
      client           = mockClient,
      location         = CloudLocation(bucketName, "prefix1/".some),
      exclude          = Set.empty,
      wildcardExcludes = Set(".indexes"),
      recursiveLevel   = 0,
      expected         = Set("prefix1/"),
    )
    check(
      client           = mockClient,
      location         = CloudLocation(bucketName, "prefix2/".some),
      recursiveLevel   = 0,
      exclude          = Set.empty,
      wildcardExcludes = Set(".indexes"),
      expected         = Set("prefix2/"),
    )
    check(
      client           = mockClient,
      location         = CloudLocation(bucketName, "prefix3/".some),
      recursiveLevel   = 1,
      exclude          = Set.empty,
      wildcardExcludes = Set(".indexes"),
      expected         = Set.empty,
    )
    check(
      client           = mockClient,
      location         = CloudLocation(bucketName, None),
      recursiveLevel   = 1,
      exclude          = Set.empty,
      wildcardExcludes = Set(".indexes"),
      expected         = Set("prefix1/", "prefix2/"),
    )
  }

  "lister" should "typical topic/partition/offset scenario" in {

    val bucketName = "typicalTopicPartitionOffsetScenario"
    val mockClient: Storage = setUpMockClient(
      bucketName,
      ".indexes/sinkName/myTopic/00005/00000000000000000050",
      ".indexes/sinkName/myTopic/00005/00000000000000000070",
      ".indexes/sinkName/myTopic/00005/00000000000000000100",
      "/my/prefix/myTopic/0000001/1.txt",
      "/my/prefix/myTopic/0000002/2.txt",
      "/my/prefix/myTopic/0000003/3.txt",
      "/my/prefix/myTopic/0000004/4.txt",
    )

    check(
      client           = mockClient,
      location         = CloudLocation(bucketName, "/my/prefix/".some),
      exclude          = Set.empty,
      wildcardExcludes = Set(".indexes"),
      recursiveLevel   = 2,
      expected = Set(
        "/my/prefix/myTopic/0000001/",
        "/my/prefix/myTopic/0000002/",
        "/my/prefix/myTopic/0000003/",
        "/my/prefix/myTopic/0000004/",
      ),
    )
  }
  "lister" should "typical topic/partition/offset scenario without trailing slash" in {

    val bucketName = "typicalTopicPartitionOffsetScenarioWithoutTrailingSlash"
    val mockClient: Storage = setUpMockClient(
      bucketName,
      ".indexes/sinkName/myTopic/00005/00000000000000000050",
      ".indexes/sinkName/myTopic/00005/00000000000000000070",
      ".indexes/sinkName/myTopic/00005/00000000000000000100",
      "/my/prefix/myTopic/0000001/1.txt",
      "/my/prefix/myTopic/0000002/2.txt",
      "/my/prefix/myTopic/0000003/3.txt",
      "/my/prefix/myTopic/0000004/4.txt",
    )

    check(
      client           = mockClient,
      location         = CloudLocation(bucketName, "/my/prefix/".some),
      exclude          = Set.empty,
      wildcardExcludes = Set(".indexes"),
      recursiveLevel   = 2,
      expected = Set(
        "/my/prefix/myTopic/0000001/",
        "/my/prefix/myTopic/0000002/",
        "/my/prefix/myTopic/0000003/",
        "/my/prefix/myTopic/0000004/",
      ),
    )
  }
  private def setUpMockClient(bucket: String, filesToSetUp: String*): Storage = {
    client.create(BucketInfo.of(bucket))
    filesToSetUp.map(file => client.create(BlobInfo.newBuilder(BlobId.of(bucket, file)).build()))
    client
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
    new GCPStorageDirectoryLister(connectorTaskId, client)
      .findDirectories(
        location,
        filesLimit,
        recursiveLevel,
        exclude,
        wildcardExcludes,
      ).unsafeRunSync() should be(expected)

}
