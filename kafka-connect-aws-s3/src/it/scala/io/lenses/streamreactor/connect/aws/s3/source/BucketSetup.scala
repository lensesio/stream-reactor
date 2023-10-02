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

import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.cloud.common.config.Format
import io.lenses.streamreactor.connect.cloud.common.config.FormatOptions
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import org.scalatest.matchers.should.Matchers

import java.io.File

class BucketSetup(implicit storageInterface: StorageInterface) extends Matchers {

  val PrefixName = "streamReactorBackups"
  val TopicName  = "myTopic"

  def setUpBucketData(bucketName: String, format: Format, formatOption: Option[FormatOptions], dir: String): Unit =
    1 to 5 foreach {
      fileNum =>
        copyResourceToBucket(
          s"/${format.entryName.toLowerCase}${generateFormatString(formatOption)}/$fileNum.${format.entryName.toLowerCase}",
          bucketName,
          s"$PrefixName/$dir/$TopicName/0/${fileNum * 200 - 1}.${format.entryName.toLowerCase}",
        )

        storageInterface.pathExists(
          bucketName,
          s"$PrefixName/$dir/$TopicName/0/${fileNum * 200 - 1}.${format.entryName.toLowerCase}",
        ) should be(Right(true))
    }

  def writeDataToBucket(
    bucketName: String,
    pathName:   String,
  ): Unit = {
    storageInterface.writeStringToFile(
      bucketName,
      pathName,
      "someData",
    )
    ()
  }

  def setUpRootBucketData(bucketName: String, format: Format, formatOption: Option[FormatOptions]): Unit =
    1 to 5 foreach {
      fileNum =>
        copyResourceToBucket(
          s"/${format.entryName.toLowerCase}${generateFormatString(formatOption)}/$fileNum.${format.entryName.toLowerCase}",
          bucketName,
          s"0/${fileNum * 200 - 1}.${format.entryName.toLowerCase}",
        )

        storageInterface.pathExists(
          bucketName,
          s"0/${fileNum * 200 - 1}.${format.entryName.toLowerCase}",
        ) should be(Right(true))

        copyResourceToBucket(
          s"/${format.entryName.toLowerCase}${generateFormatString(formatOption)}/$fileNum.${format.entryName.toLowerCase}",
          bucketName,
          s"0/${fileNum * 200 - 1}.${format.entryName.toLowerCase}",
        )

        // not really a real index file but anything that has .indexes in the name should be ignored
        writeDataToBucket(
          bucketName,
          s".indexes/00001/00000000000000000002",
        )
    }
  def totalFileLengthBytes(format: Format): Int = {
    1 to 5 map {
      fileNum: Int =>
        fileLengthBytes(
          s"/${format.entryName.toLowerCase}/$fileNum.${format.entryName.toLowerCase}",
        )
    }
  }.sum

  def generateFormatString(formatOptions: Option[FormatOptions]): String =
    formatOptions.fold("")(option => s"_${option.entryName.toLowerCase}")

  private def fileLengthBytes(
    resourceSourceFilename: String,
  ): Int = {

    val inputStream = classOf[S3ProxyContainerTest].getResourceAsStream(resourceSourceFilename)
    require(inputStream != null)
    inputStream.available()
  }

  private def copyResourceToBucket(
    resourceSourceFilename:  String,
    blobStoreContainerName:  String,
    blobStoreTargetFilename: String,
  ): Unit = {

    val resource = classOf[S3ProxyContainerTest].getResource(resourceSourceFilename)
    require(resource != null)
    val _ = storageInterface.uploadFile(
      new File(resource.getFile),
      blobStoreContainerName,
      blobStoreTargetFilename,
    )
  }

}
