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
package io.lenses.streamreactor.connect.cloud.common.formats

import io.lenses.streamreactor.connect.cloud.common.formats.reader.TextStreamReader
import io.lenses.streamreactor.connect.cloud.common.formats.writer.JsonFormatWriter
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.stream.CloudByteArrayOutputStream
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData.topic
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.UNCOMPRESSED
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.NullSinkData
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.StructSinkData

import java.io.ByteArrayInputStream
import java.time.Instant

class TextFormatStreamReaderTest extends AnyFlatSpec with Matchers {
  private implicit val compressionCodec: CompressionCodec = UNCOMPRESSED.toCodec()

  "read" should "take read through all records" in {

    val byteArrayInputStream: ByteArrayInputStream = writeRecordsToOutputStream
    val jsonTextFormatStreamReader = new TextStreamReader(byteArrayInputStream)

    jsonTextFormatStreamReader.hasNext should be(true)
    jsonTextFormatStreamReader.next() should be(SampleData.recordsAsJson(0))
    jsonTextFormatStreamReader.hasNext should be(true)
    jsonTextFormatStreamReader.next() should be(SampleData.recordsAsJson(1))
    jsonTextFormatStreamReader.hasNext should be(true)
    jsonTextFormatStreamReader.next() should be(SampleData.recordsAsJson(2))
    jsonTextFormatStreamReader.hasNext should be(false)

  }

  private def writeRecordsToOutputStream = {
    val outputStream     = new CloudByteArrayOutputStream()
    val jsonFormatWriter = new JsonFormatWriter(outputStream)
    SampleData.Users.take(3).foreach(data =>
      jsonFormatWriter.write(MessageDetail(NullSinkData(None),
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
