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
package io.lenses.streamreactor.connect.cloud.common.sink

import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.common.config.base.RetryConfig
import io.lenses.streamreactor.common.config.base.intf.ConnectionConfig
import io.lenses.streamreactor.common.errors.NoopErrorPolicy
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.traits.CloudSinkConfig
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName
import io.lenses.streamreactor.connect.cloud.common.sink.config.CloudSinkBucketOptions
import io.lenses.streamreactor.connect.cloud.common.sink.config.IndexOptions
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.writer.WriterManager
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import org.mockito.MockitoSugar
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class WriterManagerCreatorTest extends AnyFunSuite with Matchers with MockitoSugar with OptionValues {

  case class FakeConnectionConfig() extends ConnectionConfig
  case class FakeCloudSinkConfig(
    connectionConfig:              FakeConnectionConfig,
    bucketOptions:                 Seq[CloudSinkBucketOptions],
    indexOptions:                  Option[IndexOptions],
    compressionCodec:              CompressionCodec,
    connectorRetryConfig:          RetryConfig,
    errorPolicy:                   NoopErrorPolicy,
    logMetrics:                    Boolean = false,
    rolloverOnSchemaChangeEnabled: Boolean = true,
  ) extends CloudSinkConfig[FakeConnectionConfig]

  case class FakeFileMetadata(file: String, lastModified: Instant) extends FileMetadata

  implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId.apply("test", 1, 1)
  implicit val storageInterface: StorageInterface[FakeFileMetadata] =
    mock[StorageInterface[FakeFileMetadata]]

  test("create WriterManager from GCPStorageSinkConfig") {

    val config = FakeCloudSinkConfig(
      connectionConfig     = FakeConnectionConfig(),
      bucketOptions        = Seq.empty,
      indexOptions         = IndexOptions(maxIndexFiles = 10, ".indexes").some,
      compressionCodec     = CompressionCodecName.ZSTD.toCodec(),
      errorPolicy          = NoopErrorPolicy(),
      connectorRetryConfig = new RetryConfig(1, 1L, 1.0),
    )

    val writerManagerCreator          = new WriterManagerCreator[FakeFileMetadata, FakeCloudSinkConfig]()
    val (indexManager, writerManager) = writerManagerCreator.from(config)
    writerManager shouldBe a[WriterManager[_]]
    indexManager.value shouldBe a[IndexManager[_]]
  }

}
