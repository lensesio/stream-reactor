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

import io.lenses.streamreactor.connect.cloud.common.stream.CloudByteArrayOutputStream
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData.topic
import io.lenses.streamreactor.connect.cloud.common.formats.writer.ByteArraySinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.BytesFormatWriter
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.formats.writer.NullSinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.StructSinkData
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import org.apache.commons.io.IOUtils
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class BytesFormatWriterTest extends AnyFlatSpec with Matchers with EitherValues {

  private val bytes:          Array[Byte]       = getPixelBytes
  private val byteArrayValue: ByteArraySinkData = ByteArraySinkData(bytes, None)

  "round trip" should "round trip" in {
    val testBytes    = "Sausages".getBytes()
    val outputStream = new CloudByteArrayOutputStream()
    val writer       = new BytesFormatWriter(outputStream)
    writer.write(
      MessageDetail(NullSinkData(None),
                    ByteArraySinkData(testBytes),
                    Map.empty,
                    Some(Instant.now()),
                    Topic("myTopic"),
                    0,
                    Offset(0),
      ),
    )
    val result = outputStream.toByteArray
    result shouldBe testBytes
  }

  "convert" should "write a string to byte stream" in {

    val outputStream      = new CloudByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(outputStream)
    bytesFormatWriter.write(
      MessageDetail(NullSinkData(None),
                    ByteArraySinkData("Sausages".getBytes, None),
                    Map.empty,
                    Some(Instant.now()),
                    topic,
                    0,
                    Offset(0),
      ),
    )

    outputStream.toString should be("Sausages")

    bytesFormatWriter.complete()
  }

  "convert" should "write binary with ValueOnly" in {

    val outputStream      = new CloudByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(outputStream)
    bytesFormatWriter.write(MessageDetail(NullSinkData(None),
                                          byteArrayValue,
                                          Map.empty,
                                          Some(Instant.now()),
                                          topic,
                                          0,
                                          Offset(0),
    ))

    outputStream.toByteArray should be(bytes)

    bytesFormatWriter.complete()

  }

  "convert" should "not be able to write multiple parts of an image and combine" in {

    val stream = classOf[BytesFormatWriter].getResourceAsStream("/streamreactor-logo.png")
    val bytes: Array[Byte] = IOUtils.toByteArray(stream)
    val (bytes1, bytes2) = bytes.splitAt(bytes.length / 2)

    val outputStream      = new CloudByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(outputStream)
    bytesFormatWriter.write(MessageDetail(NullSinkData(None),
                                          ByteArraySinkData(bytes1, None),
                                          Map.empty,
                                          Some(Instant.now()),
                                          topic,
                                          0,
                                          Offset(0),
    )) should be(Right(()))
    bytesFormatWriter.write(MessageDetail(NullSinkData(None),
                                          ByteArraySinkData(bytes2, None),
                                          Map.empty,
                                          Some(Instant.now()),
                                          topic,
                                          0,
                                          Offset(0),
    )).left.value match {
      case e: IllegalStateException =>
        e.getMessage should be("Output stream already written to, can only write a single record for BYTES type.")
      case _ => fail("Not failed")
    }
    outputStream.toByteArray should be(bytes1)

    bytesFormatWriter.complete()
  }

  "convert" should "throw error when avro value is supplied" in {

    val outputStream      = new CloudByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(outputStream)
    val caught =
      bytesFormatWriter.write(MessageDetail(NullSinkData(None),
                                            StructSinkData(SampleData.Users.head),
                                            Map.empty,
                                            Some(Instant.now()),
                                            topic,
                                            0,
                                            Offset(0),
      ))
    bytesFormatWriter.complete()
    caught should be.leftSide
  }

  private def getPixelBytes = {
    //noinspection SpellCheckingInspection
    val stream = classOf[BytesFormatWriter].getResourceAsStream("/redpixel.gif")
    val bytes: Array[Byte] = IOUtils.toByteArray(stream)
    bytes
  }

}
