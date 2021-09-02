
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

import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocation
import io.lenses.streamreactor.connect.aws.s3.model.{Offset, StructSinkData}
import io.lenses.streamreactor.connect.aws.s3.processing.BlockingQueueProcessor
import io.lenses.streamreactor.connect.aws.s3.sink.utils.S3TestConfig
import io.lenses.streamreactor.connect.aws.s3.sink.utils.TestSampleSchemaAndData._
import io.lenses.streamreactor.connect.aws.s3.storage.stream.MultipartBlobStoreOutputStream
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AvroFormatWriterStreamTest extends AnyFlatSpec with Matchers with S3TestConfig {
  import helper._

  val avroFormatReader = new AvroFormatReader()

  implicit val queueProcessor = new BlockingQueueProcessor()

  "convert" should "write byte output stream with json for a single record" in {
    val blobStream = new MultipartBlobStoreOutputStream(RemoteS3PathLocation(BucketName, "myPrefix"), Offset(0), updateOffsetFn = (_) => () => ())

    val avroFormatWriter = new AvroFormatWriter(() => blobStream)
    avroFormatWriter.write(None, StructSinkData(users.head), topic)
    avroFormatWriter.close(RemoteS3PathLocation(BucketName, "my-path"), Offset(0))

    queueProcessor.process()

    val bytes = remoteFileAsBytes(BucketName, "my-path")

    val genericRecords = avroFormatReader.read(bytes)
    genericRecords.size should be(1)
    checkRecord(genericRecords.head, "sam", "mr", 100.43)

  }

  "convert" should "write byte output stream with json for multiple records" in {
    val blobStream = new MultipartBlobStoreOutputStream(
      RemoteS3PathLocation(BucketName, "myPrefix"), Offset(0), updateOffsetFn = (_) => () => ()
    )

    val avroFormatWriter = new AvroFormatWriter(() => blobStream)
    firstUsers.foreach(u => avroFormatWriter.write(None, StructSinkData(u), topic))
    avroFormatWriter.close(RemoteS3PathLocation(BucketName, "my-path"), Offset(0))

    queueProcessor.process() should be ('right)

    val bytes = remoteFileAsBytes(BucketName, "my-path")
    val genericRecords = avroFormatReader.read(bytes)
    genericRecords.size should be(3)

  }
}
