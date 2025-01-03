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
package io.lenses.streamreactor.connect.cloud.common.sink.writer

import cats.data.Validated
import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import org.mockito.ArgumentMatchers._
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
class WriterIndexerTest extends AnyFlatSpec with Matchers with MockitoSugar with EitherValues with BeforeAndAfter {

  private implicit val cloudLocationValidator: CloudLocationValidator = (location: CloudLocation) =>
    Validated.Valid(location)

  private val mockIndexManager: IndexManager[FileMetadata]  = mock[IndexManager[FileMetadata]]
  private val writerIndexer:    WriterIndexer[FileMetadata] = new WriterIndexer(Some(mockIndexManager))

  private val bucket = "myTestBucket"
  private val topicPartition: TopicPartition = TopicPartition(Topic("testTopic"), 1)
  private val cloudLocation:  CloudLocation  = CloudLocation(bucket, "testKey".some)

  before {
    reset(mockIndexManager)
  }

  "writeIndex" should "call IndexManager's write method with correct parameters" in {
    val uncommittedOffset = Offset(100)
    val path              = "testPath"

    when(mockIndexManager.write(anyString, anyString, any[TopicPartitionOffset])).thenReturn(Right("index"))

    val result = writerIndexer.writeIndex(topicPartition, bucket, uncommittedOffset, path)
    result.value should be(Some("index"))

    verify(mockIndexManager).write(bucket, path, topicPartition.withOffset(uncommittedOffset))
  }

  "cleanIndex" should "call IndexManager's clean method with correct parameters" in {
    val indexFileName = "indexFileName"

    when(mockIndexManager.clean(anyString, anyString, any[TopicPartition])).thenReturn(Right(1))

    val result = writerIndexer.cleanIndex(topicPartition, cloudLocation, indexFileName.some)
    result.value should be(Some(1))

    verify(mockIndexManager).clean(bucket, indexFileName, topicPartition)
  }

  "cleanIndex" should "not call IndexManager when indexFileName is None" in {
    val result = writerIndexer.cleanIndex(topicPartition, cloudLocation, none)
    result.value should be(None)

    verifyZeroInteractions(mockIndexManager)
  }

}
