
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

import io.lenses.streamreactor.connect.aws.s3.model.location.FileUtils.toBufferedOutputStream
import io.lenses.streamreactor.connect.aws.s3.model.{StructSinkData, Topic}
import io.lenses.streamreactor.connect.aws.s3.sink.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.aws.s3.sink.utils.TestSampleSchemaAndData._
import io.lenses.streamreactor.connect.aws.s3.stream.BuildLocalOutputStream
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AvroFormatWriterStreamTest extends AnyFlatSpec with Matchers with S3ProxyContainerTest {
  import helper._

  val avroFormatReader = new AvroFormatReader()

  "convert" should "write byte output stream with json for a single record" in {
    val blobStream = new BuildLocalOutputStream(toBufferedOutputStream(localFile), Topic("testTopic").withPartition(1))

    val avroFormatWriter = new AvroFormatWriter(() => blobStream)
    avroFormatWriter.write(None, StructSinkData(users.head), topic)
    avroFormatWriter.complete() should be (Right(()))
    val bytes = localFileAsBytes(localFile)

    val genericRecords = avroFormatReader.read(bytes)
    genericRecords.size should be(1)
    checkRecord(genericRecords.head, "sam", "mr", 100.43)

  }

  "convert" should "write byte output stream with json for multiple records" in {
    val blobStream = new BuildLocalOutputStream(toBufferedOutputStream(localFile), Topic("testTopic").withPartition(1))

    val avroFormatWriter = new AvroFormatWriter(() => blobStream)
    firstUsers.foreach(u => avroFormatWriter.write(None, StructSinkData(u), topic))
    avroFormatWriter.complete() should be (Right(()))

    val bytes = localFileAsBytes(localFile)
    val genericRecords = avroFormatReader.read(bytes)
    genericRecords.size should be(3)

  }
}
