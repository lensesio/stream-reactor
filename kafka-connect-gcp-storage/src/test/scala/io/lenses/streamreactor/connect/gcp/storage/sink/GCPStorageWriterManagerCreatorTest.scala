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
package io.lenses.streamreactor.connect.gcp.storage.sink

import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.sink.writer.WriterManager
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.gcp.storage.sink.config.GCPStorageSinkConfig
import io.lenses.streamreactor.connect.gcp.storage.storage.GCPStorageFileMetadata
import org.mockito.Answers
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GCPStorageWriterManagerCreatorTest extends AnyFunSuite with Matchers with MockitoSugar {

  implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId.apply("test", 1, 1)
  implicit val storageInterface: StorageInterface[GCPStorageFileMetadata] =
    mock[StorageInterface[GCPStorageFileMetadata]]

  test("create WriterManager from GCPStorageSinkConfig") {

    val config = mock[GCPStorageSinkConfig](Answers.RETURNS_DEEP_STUBS)
    when(config.offsetSeekerOptions.maxIndexFiles).thenReturn(10)
    when(config.bucketOptions).thenReturn(Seq.empty)

    val writerManager = GCPStorageWriterManagerCreator.from(config)
    writerManager shouldBe a[WriterManager[_]]
  }

}
