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

import io.lenses.streamreactor.connect.aws.s3.formats.bytes.ByteArrayUtils
import io.lenses.streamreactor.connect.aws.s3.formats.bytes.BytesWriteMode
import io.lenses.streamreactor.connect.aws.s3.formats.reader.BytesWithSizesStreamReader
import io.lenses.streamreactor.connect.aws.s3.formats.writer._
import io.lenses.streamreactor.connect.aws.s3.model.Offset
import io.lenses.streamreactor.connect.aws.s3.model.Topic
import io.lenses.streamreactor.connect.aws.s3.stream.S3ByteArrayOutputStream
import io.lenses.streamreactor.connect.aws.s3.utils.SampleData
import io.lenses.streamreactor.connect.aws.s3.utils.SampleData._
import org.apache.commons.io.IOUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream
import java.time.Instant

class BytesFormatWriterTest extends AnyFlatSpec with Matchers {

  private val bytes:            Array[Byte]       = getPixelBytes
  private val byteArrayValue:   ByteArraySinkData = ByteArraySinkData(bytes, None)
  private val pixelLengthBytes: Array[Byte]       = ByteArrayUtils.longToByteArray(bytes.length.longValue())

  "round trip" should "round trip" in {
    val testBytes    = "Sausages".getBytes()
    val outputStream = new S3ByteArrayOutputStream()
    val writer       = new BytesFormatWriter(outputStream, BytesWriteMode.ValueWithSize)
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
    val reader = new BytesWithSizesStreamReader(
      new ByteArrayInputStream(result),
      result.length.toLong,
      BytesWriteMode.ValueWithSize,
    )
    reader.next().value shouldBe testBytes
  }

  "convert" should "write a string to byte stream" in {

    val outputStream      = new S3ByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(outputStream, BytesWriteMode.ValueOnly)
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

    val outputStream      = new S3ByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(outputStream, BytesWriteMode.ValueOnly)
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

  "convert" should "write binary with KeyOnly" in {

    val outputStream      = new S3ByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(outputStream, BytesWriteMode.KeyOnly)
    bytesFormatWriter.write(
      MessageDetail(byteArrayValue,
                    ByteArraySinkData("notUsed".getBytes, None),
                    Map.empty,
                    Some(Instant.now()),
                    topic,
                    0,
                    Offset(0),
      ),
    )

    outputStream.toByteArray should be(bytes)

    bytesFormatWriter.complete()

  }

  "convert" should "write binary with KeyAndValueWithSizes" in {

    val outputStream      = new S3ByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(outputStream, BytesWriteMode.KeyAndValueWithSizes)
    bytesFormatWriter.write(MessageDetail(byteArrayValue,
                                          byteArrayValue,
                                          Map.empty,
                                          Some(Instant.now()),
                                          topic,
                                          0,
                                          Offset(0),
    ))

    outputStream.toByteArray should be(pixelLengthBytes ++ pixelLengthBytes ++ bytes ++ bytes)

    bytesFormatWriter.complete()

  }

  "convert" should "write binary with KeyWithSize" in {

    val outputStream      = new S3ByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(outputStream, BytesWriteMode.KeyWithSize)

    bytesFormatWriter.write(
      MessageDetail(byteArrayValue,
                    ByteArraySinkData("notUsed".getBytes, None),
                    Map.empty,
                    Some(Instant.now()),
                    topic,
                    0,
                    Offset(0),
      ),
    )

    outputStream.toByteArray should be(pixelLengthBytes ++ bytes)

    bytesFormatWriter.complete()

  }

  "convert" should "write binary with ValueWithSize" in {

    val outputStream      = new S3ByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(outputStream, BytesWriteMode.ValueWithSize)
    bytesFormatWriter.write(
      MessageDetail(ByteArraySinkData("notUsed".getBytes, None),
                    byteArrayValue,
                    Map.empty,
                    Some(Instant.now()),
                    topic,
                    0,
                    Offset(0),
      ),
    )

    outputStream.toByteArray should be(pixelLengthBytes ++ bytes)

    bytesFormatWriter.complete()

  }

  "convert" should "write  multiple parts of an image and combine" in {

    val stream = classOf[BytesFormatWriter].getResourceAsStream("/streamreactor-logo.png")
    val bytes: Array[Byte] = IOUtils.toByteArray(stream)
    val (bytes1, bytes2) = bytes.splitAt(bytes.length / 2)

    val outputStream      = new S3ByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(outputStream, BytesWriteMode.ValueOnly)
    bytesFormatWriter.write(MessageDetail(NullSinkData(None),
                                          ByteArraySinkData(bytes1, None),
                                          Map.empty,
                                          Some(Instant.now()),
                                          topic,
                                          0,
                                          Offset(0),
    ))
    bytesFormatWriter.write(MessageDetail(NullSinkData(None),
                                          ByteArraySinkData(bytes2, None),
                                          Map.empty,
                                          Some(Instant.now()),
                                          topic,
                                          0,
                                          Offset(0),
    ))

    outputStream.toByteArray should be(bytes)

    bytesFormatWriter.complete()
  }

  "convert" should "throw error when avro value is supplied" in {

    val outputStream      = new S3ByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(outputStream, BytesWriteMode.ValueOnly)
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
