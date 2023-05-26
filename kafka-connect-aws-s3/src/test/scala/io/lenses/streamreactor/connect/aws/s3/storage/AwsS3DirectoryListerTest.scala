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
package io.lenses.streamreactor.connect.aws.s3.storage

import cats.effect.Clock
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.s3.S3Client

class AwsS3DirectoryListerTest extends AnyFlatSpecLike with Matchers {

  private val connectorTaskId: ConnectorTaskId = ConnectorTaskId("sinkName", 1, 1)

  "dirLister" should "list all directories" in {

    val s3Client: S3Client = new MockS3Client(
      S3Page(
        "prefix1/1.txt",
        "prefix1/2.txt",
        "prefix2/3.txt",
        "prefix2/4.txt",
      ),
    )
    val directoryLister: AwsS3DirectoryLister = new AwsS3StorageInterface(connectorTaskId, s3Client)
    directoryLister.findDirectories(
      S3Location("bucket", "prefix1/".some),
      DirectoryFindCompletionConfig(1, none, none, Clock[IO]),
      Set.empty,
      Option.empty,
    ).unsafeRunSync() should be(CompletedDirectoryFindResults(
      Set("prefix1/"),
    ))
    directoryLister.findDirectories(
      S3Location("bucket"),
      DirectoryFindCompletionConfig(1, none, none, Clock[IO]),
      Set.empty,
      Option.empty,
    ).unsafeRunSync() should be(CompletedDirectoryFindResults(
      Set("prefix1/", "prefix2/"),
    ))
  }

  "dirLister" should "list multiple pages" in {

    val s3Client: S3Client = new MockS3Client(
      S3Page(
        "prefix1/1.txt",
        "prefix1/2.txt",
        "prefix2/3.txt",
        "prefix2/4.txt",
      ),
      S3Page(
        "prefix3/5.txt",
        "prefix3/6.txt",
        "prefix4/7.txt",
        "prefix4/8.txt",
      ),
    )
    val directoryLister: AwsS3DirectoryLister = new AwsS3StorageInterface(connectorTaskId, s3Client)
    directoryLister.findDirectories(
      S3Location("bucket", none),
      DirectoryFindCompletionConfig(1, none, none, Clock[IO]),
      Set.empty,
      Option.empty,
    ).unsafeRunSync() should be(CompletedDirectoryFindResults(
      Set("prefix1/", "prefix2/", "prefix3/", "prefix4/"),
    ))
  }

  "dirLister" should "exclude directories" in {

    val s3Client: S3Client = new MockS3Client(
      S3Page(
        "prefix1/1.txt",
        "prefix1/2.txt",
        "prefix2/3.txt",
        "prefix2/4.txt",
      ),
      S3Page(
        "prefix3/5.txt",
        "prefix3/6.txt",
        "prefix4/7.txt",
        "prefix4/8.txt",
      ),
    )
    val directoryLister: AwsS3DirectoryLister = new AwsS3StorageInterface(connectorTaskId, s3Client)
    directoryLister.findDirectories(
      S3Location("bucket", none),
      DirectoryFindCompletionConfig(1, none, none, Clock[IO]),
      Set("prefix1/", "prefix4/"),
      Option.empty,
    ).unsafeRunSync() should be(CompletedDirectoryFindResults(
      Set("prefix2/", "prefix3/"),
    ))
  }

  "dirLister" should "return after first page of prefixes found" in {

    val s3Client: S3Client = new MockS3Client(
      S3Page(
        "prefix1/1.txt",
        "prefix1/2.txt",
        "prefix2/3.txt",
        "prefix2/4.txt",
      ),
      S3Page(
        "prefix3/5.txt",
        "prefix3/6.txt",
        "prefix4/7.txt",
        "prefix4/8.txt",
      ),
    )
    val directoryLister: AwsS3DirectoryLister = new AwsS3StorageInterface(connectorTaskId, s3Client)
    directoryLister.findDirectories(
      S3Location("bucket", none),
      DirectoryFindCompletionConfig(1, 1.some, none, Clock[IO]),
      Set.empty,
      Option.empty,
    ).unsafeRunSync() should be(PausedDirectoryFindResults(
      Set("prefix1/", "prefix2/"),
      "p",
      "prefix2/4.txt",
    ))
  }

}
