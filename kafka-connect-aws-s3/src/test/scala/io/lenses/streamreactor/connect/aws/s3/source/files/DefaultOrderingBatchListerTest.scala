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
import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3RootLocation
import io.lenses.streamreactor.connect.aws.s3.storage.FileListError
import io.lenses.streamreactor.connect.aws.s3.storage.ListResponse
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DefaultOrderingBatchListerTest
    extends AnyFlatSpec
    with Matchers
    with MockitoSugar
    with BeforeAndAfter
    with OptionValues
    with EitherValues {

  private val pathLocation     = RemoteS3RootLocation("bucket:path").toPath()
  private val storageInterface = mock[StorageInterface]

  private val listerFn = DefaultOrderingBatchLister.listBatch(storageInterface, pathLocation, 10) _

  "listBatch" should "return first result when no TopicPartitionOffset has been provided" in {

    val serviceResponse: ListResponse[String] = mock[ListResponse[String]]
    when(storageInterface.list(pathLocation, None, 10))
      .thenReturn(serviceResponse.some.asRight)

    listerFn(none).value.value should be(serviceResponse)
  }

  "listBatch" should "return empty when no results are found" in {

    when(storageInterface.list(pathLocation, None, 10)).thenReturn(
      none.asRight,
    )

    listerFn(none).value should be(none)

  }

  "listBatch" should "pass through any errors" in {
    val exception = FileListError(new IllegalStateException("BadThingsHappened"), "Oh No")

    when(storageInterface.list(pathLocation, None, 10)).thenReturn(
      exception.asLeft,
    )
    listerFn(none).left.value should be(exception)

  }

}
