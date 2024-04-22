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
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3SinkConfig
import io.lenses.streamreactor.connect.aws.s3.storage.AwsS3StorageInterface
import io.lenses.streamreactor.connect.aws.s3.storage.S3FileMetadata
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushCount
import io.lenses.streamreactor.connect.cloud.common.sink.CoreSinkTaskTestCases
import software.amazon.awssdk.services.s3.S3Client

import scala.jdk.CollectionConverters.MapHasAsJava

class S3SinkTaskTest
    extends CoreSinkTaskTestCases[S3FileMetadata, AwsS3StorageInterface, S3SinkConfig, S3Client, S3SinkTask](
      "S3SinkTask",
    )
    with S3ProxyContainerTest {

  "S3SinkTask" should "fail with message when deprecated properties are used" in {
    Resource.make(IO(createSinkTask())) { sinkTask =>
      IO(sinkTask.stop())
    }.use { sinkTask =>
      IO {
        val exMessage = intercept[IllegalArgumentException] {
          sinkTask.start(
            (defaultProps ++ Map(
              "aws.access.key" -> "myAccessKey",
              s"$prefix.kcql"  -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `CSV` PROPERTIES('${FlushCount.entryName}'=1)",
            )).asJava,
          )
        }.getMessage

        exMessage should startWith("The following properties have been deprecated: `aws.access.key`")
        exMessage should include("Change `aws.access.key` to `connect.s3.aws.access.key`")

        listBucketPath(BucketName, "streamReactorBackups/").size should be(0)
      }
    }.unsafeRunSync()
  }

}
