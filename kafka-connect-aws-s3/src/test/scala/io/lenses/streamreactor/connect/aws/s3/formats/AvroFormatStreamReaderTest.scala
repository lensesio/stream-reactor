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

import io.lenses.streamreactor.connect.aws.s3.model.StructSinkData
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocation
import io.lenses.streamreactor.connect.aws.s3.utils.TestSampleSchemaAndData.checkRecord
import io.lenses.streamreactor.connect.aws.s3.utils.TestSampleSchemaAndData.firstUsers
import io.lenses.streamreactor.connect.aws.s3.utils.TestSampleSchemaAndData.topic
import io.lenses.streamreactor.connect.aws.s3.stream.S3ByteArrayOutputStream
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream

class AvroFormatStreamReaderTest extends AnyFlatSpec with Matchers {

  "read" should "read through all records" in {

    val byteArrayInputStream: ByteArrayInputStream = writeRecordsToOutputStream
    val avroFormatStreamReader =
      new AvroFormatStreamReader(() => byteArrayInputStream, RemoteS3PathLocation("test-bucket", "test-path"))

    avroFormatStreamReader.hasNext should be(true)
    checkRecord(avroFormatStreamReader.next().data, "sam", Some("mr"), 100.43)
    avroFormatStreamReader.hasNext should be(true)
    checkRecord(avroFormatStreamReader.next().data, "laura", Some("ms"), 429.06)
    avroFormatStreamReader.hasNext should be(true)
    checkRecord(avroFormatStreamReader.next().data, "tom", None, 395.44)
    avroFormatStreamReader.hasNext should be(false)

  }

  private def writeRecordsToOutputStream = {
    val outputStream     = new S3ByteArrayOutputStream()
    val avroFormatWriter = new AvroFormatWriter(() => outputStream)
    firstUsers.foreach(str => avroFormatWriter.write(None, StructSinkData(str), topic))
    avroFormatWriter.complete()

    new ByteArrayInputStream(outputStream.toByteArray)
  }
}
