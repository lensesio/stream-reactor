/*
 * Copyright 2017-2024 Lenses.io Ltd
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

import io.lenses.streamreactor.connect.cloud.common.formats.reader.AvroStreamReader
import io.lenses.streamreactor.connect.cloud.common.formats.writer.AvroFormatWriter
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.formats.writer.NullSinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.StructSinkData
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.UNCOMPRESSED
import io.lenses.streamreactor.connect.cloud.common.stream.CloudByteArrayOutputStream
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData.topic
import org.apache.kafka.connect.data.Struct
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream
import java.time.Instant

class AvroFormatStreamReaderTest extends AnyFlatSpec with Matchers {

  private implicit val compressionCodec: CompressionCodec = UNCOMPRESSED.toCodec()

  "read" should "read through all records" in {

    val byteArrayInputStream: ByteArrayInputStream = writeRecordsToOutputStream
    val avroFormatStreamReader =
      new AvroStreamReader(
        byteArrayInputStream,
      )

    avroFormatStreamReader.hasNext should be(true)
    val actual1 = avroFormatStreamReader.next().value().asInstanceOf[Struct]
    actual1.get("name") shouldBe "sam"
    actual1.get("title") shouldBe "mr"
    actual1.get("salary") shouldBe 100.43

    avroFormatStreamReader.hasNext should be(true)

    val actual2 = avroFormatStreamReader.next().value().asInstanceOf[Struct]
    actual2.get("name") shouldBe "laura"
    actual2.get("title") shouldBe "ms"
    actual2.get("salary") shouldBe 429.06

    avroFormatStreamReader.hasNext should be(true)
    val actual3 = avroFormatStreamReader.next().value().asInstanceOf[Struct]
    actual3.get("name") shouldBe "tom"
    actual3.get("title") shouldBe null
    actual3.get("salary") shouldBe 395.44
    avroFormatStreamReader.hasNext should be(false)

  }

  private def writeRecordsToOutputStream = {
    val outputStream     = new CloudByteArrayOutputStream()
    val avroFormatWriter = new AvroFormatWriter(outputStream)
    SampleData.Users.take(3).foreach(str =>
      avroFormatWriter.write(MessageDetail(NullSinkData(None),
                                           StructSinkData(str),
                                           Map.empty,
                                           Some(Instant.now()),
                                           topic,
                                           0,
                                           Offset(0),
      )),
    )
    avroFormatWriter.complete()

    new ByteArrayInputStream(outputStream.toByteArray)
  }
}
