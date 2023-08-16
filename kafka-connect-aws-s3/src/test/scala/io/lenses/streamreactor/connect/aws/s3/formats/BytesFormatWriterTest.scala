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

import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.aws.s3.formats.bytes.ByteArrayUtils
import io.lenses.streamreactor.connect.aws.s3.formats.bytes.BytesWriteMode
import io.lenses.streamreactor.connect.aws.s3.formats.reader.ByteArraySourceData
import io.lenses.streamreactor.connect.aws.s3.formats.reader.BytesFormatWithSizesStreamReader
import io.lenses.streamreactor.connect.aws.s3.formats.writer._
import io.lenses.streamreactor.connect.aws.s3.model.Topic
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.stream.S3ByteArrayOutputStream
import io.lenses.streamreactor.connect.aws.s3.utils.TestSampleSchemaAndData._
import org.apache.commons.io.IOUtils
import org.mockito.MockitoSugar.mock
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream

class BytesFormatWriterTest extends AnyFlatSpec with Matchers {

  private val bytes:            Array[Byte]       = getPixelBytes
  private val byteArrayValue:   ByteArraySinkData = ByteArraySinkData(bytes, None)
  private val pixelLengthBytes: Array[Byte]       = ByteArrayUtils.longToByteArray(bytes.length.longValue())

  "Value with size" should "round trip" in {
    val test = new TestFixture[String] {
      override protected val mode: BytesWriteMode = BytesWriteMode.ValueWithSize
      override protected val data: List[String]   = List("Sausages")
      override protected def getRecord(value: String): (Option[SinkData], SinkData) =
        None -> ByteArraySinkData(value.getBytes, None)
      override protected def assertResult(result: ByteArraySourceData, expected: String): Assertion =
        result.data.value shouldBe expected.getBytes()
    }
    test.run()
  }

  "Values with Size" should "round trip" in {
    val test = new TestFixture[Option[String]] {
      override protected val mode: BytesWriteMode = BytesWriteMode.ValueWithSize
      override protected val data: List[Option[String]] = List(
        "record 1 value".some,
        "record 2 value".some,
        "record 3 value".some,
        None,
      )

      override protected def getRecord(value: Option[String]): (Option[SinkData], SinkData) =
        None -> toSinkData(value)

      override protected def assertResult(result: ByteArraySourceData, expected: Option[String]): Assertion = {
        convertToString(result.data.key) shouldBe None
        convertToString(result.data.value) shouldBe expected
      }
    }
    test.run()
  }

  "Keys with Size" should "round trip" in {
    val test = new TestFixture[Option[String]] {
      override protected val mode: BytesWriteMode = BytesWriteMode.KeyWithSize
      override protected val data: List[Option[String]] = List(
        "record 1 value".some,
        "record 2 value".some,
        "record 3 value".some,
        None,
      )

      override protected def getRecord(value: Option[String]): (Option[SinkData], SinkData) =
        toSinkData(value).some -> NullSinkData(None)

      override protected def assertResult(result: ByteArraySourceData, expected: Option[String]): Assertion = {
        convertToString(result.data.key) shouldBe expected
        convertToString(result.data.value) shouldBe None
      }
    }
    test.run()
  }

  "Values and Keys" should "round trip" in {
    val test = new TestFixture[(Option[String], Option[String])] {
      override protected val mode: BytesWriteMode = BytesWriteMode.KeyAndValueWithSizes
      override protected val data: List[(Option[String], Option[String])] = List(
        Option.empty[String] -> "record 1 value".some,
        "record 2 key".some  -> "record 2 value".some,
        None                 -> "record 3 value".some,
        "record 4 key".some  -> None,
      )

      override protected def getRecord(value: (Option[String], Option[String])): (Option[SinkData], SinkData) = {
        val (kv, vv) = value
        val k: Option[SinkData] = kv.map(v => ByteArraySinkData(v.getBytes(), None).asInstanceOf[SinkData])
        val v: SinkData         = toSinkData(vv)
        k -> v
      }

      override protected def assertResult(
        result:   ByteArraySourceData,
        expected: (Option[String], Option[String]),
      ): Assertion = {
        convertToString(result.data.key) shouldBe expected._1
        convertToString(result.data.value) shouldBe expected._2
      }
    }
    test.run()
  }

  "convert" should "write a string to byte stream" in {

    val outputStream      = new S3ByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(outputStream, BytesWriteMode.ValueOnly)
    bytesFormatWriter.write(None, ByteArraySinkData("Sausages".getBytes, None), topic)

    outputStream.toString should be("Sausages")

    bytesFormatWriter.complete()
  }

  "convert" should "write binary with ValueOnly" in {

    val outputStream      = new S3ByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(outputStream, BytesWriteMode.ValueOnly)
    bytesFormatWriter.write(None, byteArrayValue, topic)

    outputStream.toByteArray should be(bytes)

    bytesFormatWriter.complete()

  }

  "convert" should "write binary with KeyOnly" in {

    val outputStream      = new S3ByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(outputStream, BytesWriteMode.KeyOnly)
    bytesFormatWriter.write(Some(byteArrayValue), ByteArraySinkData("notUsed".getBytes, None), topic)

    outputStream.toByteArray should be(bytes)

    bytesFormatWriter.complete()

  }

  "convert" should "write binary with KeyAndValueWithSizes" in {

    val outputStream      = new S3ByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(outputStream, BytesWriteMode.KeyAndValueWithSizes)
    bytesFormatWriter.write(Some(byteArrayValue), byteArrayValue, topic)

    outputStream.toByteArray should be(pixelLengthBytes ++ pixelLengthBytes ++ bytes ++ bytes)

    bytesFormatWriter.complete()

  }

  "convert" should "write binary with KeyWithSize" in {

    val outputStream      = new S3ByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(outputStream, BytesWriteMode.KeyWithSize)

    bytesFormatWriter.write(Some(byteArrayValue), ByteArraySinkData("notUsed".getBytes, None), topic)

    outputStream.toByteArray should be(pixelLengthBytes ++ bytes)

    bytesFormatWriter.complete()

  }

  "convert" should "write binary with ValueWithSize" in {

    val outputStream      = new S3ByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(outputStream, BytesWriteMode.ValueWithSize)
    bytesFormatWriter.write(Some(ByteArraySinkData("notUsed".getBytes, None)), byteArrayValue, topic)

    outputStream.toByteArray should be(pixelLengthBytes ++ bytes)

    bytesFormatWriter.complete()

  }

  "convert" should "write  multiple parts of an image and combine" in {

    val stream = classOf[BytesFormatWriter].getResourceAsStream("/streamreactor-logo.png")
    val bytes: Array[Byte] = IOUtils.toByteArray(stream)
    val (bytes1, bytes2) = bytes.splitAt(bytes.length / 2)

    val outputStream      = new S3ByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(outputStream, BytesWriteMode.ValueOnly)
    bytesFormatWriter.write(None, ByteArraySinkData(bytes1, None), topic)
    bytesFormatWriter.write(None, ByteArraySinkData(bytes2, None), topic)

    outputStream.toByteArray should be(bytes)

    bytesFormatWriter.complete()
  }

  "convert" should "throw error when avro value is supplied" in {

    val outputStream      = new S3ByteArrayOutputStream()
    val bytesFormatWriter = new BytesFormatWriter(outputStream, BytesWriteMode.ValueOnly)
    val caught            = bytesFormatWriter.write(None, StructSinkData(users.head), topic)
    bytesFormatWriter.complete()
    caught should be.leftSide
  }

  private def getPixelBytes = {
    //noinspection SpellCheckingInspection
    val stream = classOf[BytesFormatWriter].getResourceAsStream("/redpixel.gif")
    val bytes: Array[Byte] = IOUtils.toByteArray(stream)
    bytes
  }

  private sealed trait TestFixture[T] {
    protected val mode: BytesWriteMode
    protected val data: List[T]
    protected def getRecord(value:     T): (Option[SinkData], SinkData)
    protected def assertResult(result: ByteArraySourceData, expected: T): Assertion
    def run(): Unit = {
      val outputStream = new S3ByteArrayOutputStream()
      val writer       = new BytesFormatWriter(outputStream, mode)
      val topic        = Topic("myTopic")
      data.foreach { value =>
        val (k, v) = getRecord(value)
        writer.write(k, v, topic)
      }

      val result = outputStream.toByteArray

      val reader = new BytesFormatWithSizesStreamReader(
        new ByteArrayInputStream(result),
        result.length.toLong,
        mock[S3Location],
        mode,
      )
      data.foreach { value =>
        withClue(s"value: $value") {
          val byteArraySourceData = reader.next()
          assertResult(byteArraySourceData, value)
        }
      }
    }
  }

  private def toSinkData(maybe: Option[String]): SinkData =
    maybe.fold(NullSinkData(None).asInstanceOf[SinkData])(v => ByteArraySinkData(v.getBytes, None))

  private def convertToString(array: Array[Byte]): Option[String] = Option(array).filter(_.nonEmpty).map(new String(_))
}
