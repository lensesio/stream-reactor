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
package io.lenses.streamreactor.connect.aws.s3.formats

import io.lenses.streamreactor.connect.aws.s3.formats.reader.StringSourceData
import io.lenses.streamreactor.connect.aws.s3.formats.reader.TextFormatStreamReader
import io.lenses.streamreactor.connect.aws.s3.formats.writer.JsonFormatWriter
import io.lenses.streamreactor.connect.aws.s3.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.aws.s3.formats.writer.StructSinkData
import io.lenses.streamreactor.connect.aws.s3.model.Offset
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.stream.S3ByteArrayOutputStream
import io.lenses.streamreactor.connect.aws.s3.utils.SampleData
import io.lenses.streamreactor.connect.aws.s3.utils.SampleData.topic
import org.mockito.MockitoSugar.mock
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream
import java.time.Instant

class TextFormatStreamReaderTest extends AnyFlatSpec with Matchers {

  "read" should "take read through all records" in {

    val byteArrayInputStream: ByteArrayInputStream = writeRecordsToOutputStream
    val avroFormatStreamReader =
      new TextFormatStreamReader(byteArrayInputStream, mock[S3Location])

    avroFormatStreamReader.hasNext should be(true)
    avroFormatStreamReader.next() should be(StringSourceData(SampleData.recordsAsJson(0), 0))
    avroFormatStreamReader.hasNext should be(true)
    avroFormatStreamReader.next() should be(StringSourceData(SampleData.recordsAsJson(1), 1))
    avroFormatStreamReader.hasNext should be(true)
    avroFormatStreamReader.next() should be(StringSourceData(SampleData.recordsAsJson(2), 2))
    avroFormatStreamReader.hasNext should be(false)

  }

  private def writeRecordsToOutputStream = {
    val outputStream     = new S3ByteArrayOutputStream()
    val jsonFormatWriter = new JsonFormatWriter(outputStream)
    SampleData.Users.take(3).foreach(data =>
      jsonFormatWriter.write(MessageDetail(None,
                                           StructSinkData(data),
                                           Map.empty,
                                           Some(Instant.now()),
                                           topic,
                                           0,
                                           Offset(0),
      )),
    )
    jsonFormatWriter.complete()

    val byteArrayInputStream = new ByteArrayInputStream(outputStream.toByteArray)
    byteArrayInputStream
  }
}
