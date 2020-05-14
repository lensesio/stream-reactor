
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

package io.lenses.streamreactor.connect.aws.s3.formats

import io.lenses.streamreactor.connect.aws.s3.BucketAndPath
import io.lenses.streamreactor.connect.aws.s3.sink.utils.TestSampleSchemaAndData._
import io.lenses.streamreactor.connect.aws.s3.sink.utils.{S3TestConfig, S3TestPayloadReader}
import io.lenses.streamreactor.connect.aws.s3.storage.MultipartBlobStoreOutputStream
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ParquetFormatWriterStreamTest extends AnyFlatSpec with Matchers with S3TestConfig {

  val parquetFormatReader = new ParquetFormatReader()

  "convert" should "write byteoutputstream with json for a single record" in {
    val blobStream = new MultipartBlobStoreOutputStream(BucketAndPath(BucketName, "myPrefix"), 20000)(storageInterface)

    val parquetFormatWriter = new ParquetFormatWriter(() => blobStream)
    parquetFormatWriter.write(users(0), topic)
    parquetFormatWriter.close()

    val bytes = S3TestPayloadReader.readPayload(BucketName, "myPrefix", blobStoreContext)

    val genericRecords = parquetFormatReader.read(bytes)
    genericRecords.size should be(1)
    checkRecord(genericRecords(0), "sam", "mr", 100.43)

  }

  "convert" should "write byteoutputstream with json for multiple records" in {
    val blobStream = new MultipartBlobStoreOutputStream(BucketAndPath(BucketName, "myPrefix"), 100)(storageInterface)

    val parquetFormatWriter = new ParquetFormatWriter(() => blobStream)
    users.foreach(parquetFormatWriter.write(_, topic))
    parquetFormatWriter.close()

    val bytes = S3TestPayloadReader.readPayload(BucketName, "myPrefix", blobStoreContext)
    val genericRecords = parquetFormatReader.read(bytes)
    genericRecords.size should be(3)

  }
}
