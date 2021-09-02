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

package io.lenses.streamreactor.connect.aws.s3.source

import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._
import io.lenses.streamreactor.connect.aws.s3.config.{AuthMode, Format, FormatOptions}
import io.lenses.streamreactor.connect.aws.s3.model.location.{LocalPathLocation, RemoteS3PathLocation}
import io.lenses.streamreactor.connect.aws.s3.sink.utils.{S3ProxyContext, S3TestConfig, RemoteFileTestHelper}
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import org.scalatest.matchers.should.Matchers


class BucketSetup(implicit storageInterface: StorageInterface) extends Matchers {

  import S3ProxyContext._

  val DefaultProps = Map(
    AWS_ACCESS_KEY -> Identity,
    AWS_SECRET_KEY -> Credential,
    AUTH_MODE -> AuthMode.Credentials.toString,
    CUSTOM_ENDPOINT -> S3ProxyContext.Uri,
    ENABLE_VIRTUAL_HOST_BUCKETS -> "true"
  )

  val PrefixName = "streamReactorBackups"
  val TopicName = "myTopic"

  def setUpBucketData(bucketName: String, format: Format, formatOption: Option[FormatOptions]): Unit = {

    1 to 5 foreach {
      fileNum =>
        copyResourceToBucket(
          s"/${format.entryName.toLowerCase}${generateFormatString(formatOption)}/$fileNum.${format.entryName.toLowerCase}",
          bucketName,
          s"$PrefixName/$TopicName/0/${fileNum * 200 - 1}.${format.entryName.toLowerCase}"
        )

        storageInterface.pathExists(RemoteS3PathLocation(bucketName, s"$PrefixName/$TopicName/0/${fileNum * 200 - 1}.${format.entryName.toLowerCase}"))should be(true)
    }
  }

  def totalFileLengthBytes(format: Format, formatOption: Option[FormatOptions]): Int = {
    1 to 5 map {
      fileNum: Int =>
        fileLengthBytes(
          s"/${format.entryName.toLowerCase}${generateFormatString(formatOption)}/$fileNum.${format.entryName.toLowerCase}",
        )
    }
  }.seq.sum

  def generateFormatString(formatOptions: Option[FormatOptions]): String = {
    formatOptions.fold("")(option => s"_${option.entryName.toLowerCase}")
  }

  private def fileLengthBytes(
                       resourceSourceFilename: String
                     ): Int = {

    val inputStream = classOf[S3TestConfig].getResourceAsStream(resourceSourceFilename)
    require(inputStream != null)
    inputStream.available()
  }

  private def copyResourceToBucket(
                            resourceSourceFilename: String,
                            blobStoreContainerName: String,
                            blobStoreTargetFilename: String,
                          ): Unit = {

    val resource = classOf[S3TestConfig].getResource(resourceSourceFilename)
    require(resource != null)
    storageInterface.uploadFile(
      LocalPathLocation(resource.getFile),
      RemoteS3PathLocation(blobStoreContainerName, blobStoreTargetFilename)
    )
  }

}
