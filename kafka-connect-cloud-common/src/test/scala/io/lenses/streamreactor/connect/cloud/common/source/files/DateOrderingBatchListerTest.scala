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
package io.lenses.streamreactor.connect.cloud.common.source.files

import cats.implicits._
import io.lenses.streamreactor.connect.cloud.common.sink.seek.TestFileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.FileListError
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfKeysResponse
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfMetadataResponse
import io.lenses.streamreactor.connect.cloud.common.storage.ListResponse
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import org.mockito.ArgumentMatchersSugar._
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.time.temporal.ChronoUnit

class DateOrderingBatchListerTest
    extends AnyFlatSpec
    with Matchers
    with MockitoSugar
    with BeforeAndAfter
    with OptionValues
    with EitherValues {

  private val bucket:           String                             = "bucket"
  private val prefix:           String                             = "prefix"
  private val storageInterface: StorageInterface[TestFileMetadata] = mock[StorageInterface[TestFileMetadata]]

  private val NumResults: Int = 10
  private val listerFn: Option[TestFileMetadata] => Either[
    FileListError,
    Option[ListResponse[String, TestFileMetadata]],
  ] =
    DateOrderingBatchLister.listBatch[TestFileMetadata](storageInterface, bucket, prefix.some, NumResults)
  private val instBase: Instant = Instant.now
  private def instant(no: Int): Instant = {
    require(no <= 100)
    instBase.minus((100 - no).toLong, ChronoUnit.DAYS)
  }
  private val allFiles = (1 to 100).map(i => TestFileMetadata(s"/file$i", instant(i)))

  "listBatch" should "return first result when no TopicPartitionOffset has been provided" in {
    val returnFiles = allFiles.slice(0, 10)
    val storageInterfaceResponse: ListOfMetadataResponse[TestFileMetadata] = ListOfMetadataResponse[TestFileMetadata](
      bucket,
      prefix.some,
      returnFiles,
      returnFiles.last,
    )

    mockStorageInterface(storageInterfaceResponse)

    listerFn(none).value.value should be(ListOfKeysResponse[TestFileMetadata](
      bucket,
      prefix.some,
      returnFiles.map(_.file),
      returnFiles.last,
    ))
  }

  "listBatch" should "return empty when no results are found" in {

    when(
      storageInterface.listFileMetaRecursive(
        eqTo(bucket),
        eqTo(prefix.some),
      ),
    ).thenReturn(none.asRight)

    listerFn(none) should be(none.asRight)
  }

  "listBatch" should "pass through any errors" in {
    val exception = FileListError(new IllegalStateException("BadThingsHappened"), bucket, prefix.some)

    when(
      storageInterface.listFileMetaRecursive(
        eqTo(bucket),
        eqTo(prefix.some),
      ),
    ).thenReturn(
      exception.asLeft,
    )
    listerFn(none).left.value should be(exception)

  }

  "listBatch" should "sort allFiles in order" in {
    val returnFiles = allFiles.slice(0, 10)
    val storageInterfaceResponse: ListOfMetadataResponse[TestFileMetadata] = ListOfMetadataResponse[TestFileMetadata](
      bucket,
      prefix.some,
      returnFiles.reverse,
      returnFiles.head,
    )

    mockStorageInterface(storageInterfaceResponse)

    listerFn(none).value.value should be(ListOfKeysResponse(
      bucket,
      prefix.some,
      returnFiles.map(_.file),
      returnFiles.last,
    ))
  }

  "listBatch" should "return requested number of results in order" in {
    val storageInterfaceResponse: ListOfMetadataResponse[TestFileMetadata] = ListOfMetadataResponse[TestFileMetadata](
      bucket,
      prefix.some,
      allFiles.reverse,
      allFiles.head,
    )

    mockStorageInterface(storageInterfaceResponse)

    listerFn(none).value.value should be(ListOfKeysResponse(
      bucket,
      prefix.some,
      allFiles.take(NumResults).map(_.file),
      allFiles(9),
    ))
  }

  "listBatch" should "resume from the next file in date order" in {
    val storageInterfaceResponse: ListOfMetadataResponse[TestFileMetadata] = ListOfMetadataResponse[TestFileMetadata](
      bucket,
      prefix.some,
      allFiles.reverse,
      allFiles.head,
    )

    mockStorageInterface(storageInterfaceResponse)

    listerFn(allFiles(9).some).value.value should be(ListOfKeysResponse(
      bucket,
      prefix.some,
      allFiles.slice(10, 20).map(_.file),
      allFiles(19),
    ))
    listerFn(allFiles(19).some).value.value should be(ListOfKeysResponse(
      bucket,
      prefix.some,
      allFiles.slice(20, 30).map(_.file),
      allFiles(29),
    ))
  }
  private def mockStorageInterface(storageInterfaceResponse: ListOfMetadataResponse[TestFileMetadata]) =
    when(
      storageInterface.listFileMetaRecursive(
        eqTo(bucket),
        eqTo(prefix.some),
      ),
    ).thenReturn(storageInterfaceResponse.some.asRight)
}
