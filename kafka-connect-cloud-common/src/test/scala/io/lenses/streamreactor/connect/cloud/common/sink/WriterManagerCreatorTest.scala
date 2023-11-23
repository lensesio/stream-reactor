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
package io.lenses.streamreactor.connect.cloud.common.sink

import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName
import io.lenses.streamreactor.connect.cloud.common.sink.config.CloudSinkBucketOptions
import io.lenses.streamreactor.connect.cloud.common.sink.config.CloudSinkConfig
import io.lenses.streamreactor.connect.cloud.common.sink.config.OffsetSeekerOptions
import io.lenses.streamreactor.connect.cloud.common.sink.writer.WriterManager
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class WriterManagerCreatorTest extends AnyFunSuite with Matchers with MockitoSugar {

  case class FakeCloudSinkConfig(
    bucketOptions:       Seq[CloudSinkBucketOptions],
    offsetSeekerOptions: OffsetSeekerOptions,
    compressionCodec:    CompressionCodec,
  ) extends CloudSinkConfig

  case class FakeFileMetadata(file: String, lastModified: Instant) extends FileMetadata

  implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId.apply("test", 1, 1)
  implicit val storageInterface: StorageInterface[FakeFileMetadata] =
    mock[StorageInterface[FakeFileMetadata]]

  test("create WriterManager from GCPStorageSinkConfig") {

    val config = FakeCloudSinkConfig(
      bucketOptions       = Seq.empty,
      offsetSeekerOptions = OffsetSeekerOptions(maxIndexFiles = 10),
      compressionCodec    = CompressionCodecName.ZSTD.toCodec(),
    )

    val writerManagerCreator = new WriterManagerCreator[FakeFileMetadata, FakeCloudSinkConfig]()
    val writerManager        = writerManagerCreator.from(config)
    writerManager shouldBe a[WriterManager[_]]
  }

}
