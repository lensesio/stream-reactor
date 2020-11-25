
/*
 * Copyright 2020 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.sink

import io.lenses.streamreactor.connect.aws.s3.config.Format.Json
import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.model.{BucketAndPath, Offset, Topic, TopicPartitionOffset}
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OffsetSeekerTest extends AnyFlatSpec with MockitoSugar with Matchers {

  private implicit val fileNamingStrategy = new HierarchicalS3FileNamingStrategy(FormatSelection(Json))
  private val offsetSeeker = new OffsetSeeker

  private implicit val storageInterface: StorageInterface = mock[StorageInterface]

  private val bucketAndPath = BucketAndPath("my-bucket", "path/myTopic/0")

  "seek" should "return empty set when path does not exist" in {

    when(storageInterface.pathExists(bucketAndPath)).thenReturn(false)

    offsetSeeker.seek(bucketAndPath) should be(None)
  }

  "seek" should "return expected offsets for 1 filename" in {

    when(storageInterface.pathExists(bucketAndPath)).thenReturn(true)
    when(storageInterface.fetchLatest(bucketAndPath)).thenReturn(Some("path/myTopic/0/100.json"))

    offsetSeeker.seek(bucketAndPath) should be(Some(TopicPartitionOffset(Topic("myTopic"), 0, Offset(100))))
  }

}
