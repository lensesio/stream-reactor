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

import io.lenses.streamreactor.connect.aws.s3.config.Format.Parquet
import io.lenses.streamreactor.connect.aws.s3.formats.reader.ParquetFormatReader
import io.lenses.streamreactor.connect.aws.s3.formats.writer._
import io.lenses.streamreactor.connect.aws.s3.model.CompressionCodecName.BROTLI
import io.lenses.streamreactor.connect.aws.s3.model.CompressionCodecName.LZ4
import io.lenses.streamreactor.connect.aws.s3.model.CompressionCodecName.LZO
import io.lenses.streamreactor.connect.aws.s3.model.CompressionCodecName.UNCOMPRESSED
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.model.location.FileUtils.toBufferedOutputStream
import io.lenses.streamreactor.connect.aws.s3.stream.BuildLocalOutputStream
import io.lenses.streamreactor.connect.aws.s3.utils.ITSampleSchemaAndData._
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ParquetFormatWriterStreamTest extends AnyFlatSpec with Matchers with S3ProxyContainerTest with EitherValues {
  import helper._

  implicit val compressionCodec: CompressionCodec = UNCOMPRESSED.toCodec()

  val parquetFormatReader = new ParquetFormatReader()

  "convert" should "write byte output stream with json for a single record" in {

    implicit val compressionCodec: CompressionCodec = UNCOMPRESSED.toCodec()

    val blobStream          = new BuildLocalOutputStream(toBufferedOutputStream(localFile), Topic("testTopic").withPartition(1))
    val parquetFormatWriter = new ParquetFormatWriter(blobStream)
    parquetFormatWriter.write(None, StructSinkData(users.head), topic)
    parquetFormatWriter.getPointer should be(21)
    parquetFormatWriter.write(None, StructSinkData(users(1)), topic)
    parquetFormatWriter.getPointer should be(44)
    parquetFormatWriter.write(None, StructSinkData(users(2)), topic)
    parquetFormatWriter.getPointer should be(59)
    parquetFormatWriter.complete() should be(Right(()))

    val bytes = localFileAsBytes(localFile)

    val genericRecords = parquetFormatReader.read(bytes)
    genericRecords.size should be(3)
    checkRecord(genericRecords.head, "sam", "mr", 100.43)

  }

  "convert" should "write byte output stream with json for multiple records" in {
    writeToParquetFile

    val bytes          = localFileAsBytes(localFile)
    val genericRecords = parquetFormatReader.read(bytes)
    genericRecords.size should be(3)

  }

  "convert" should "throw an error when writing array without schema" in {

    val blobStream          = new BuildLocalOutputStream(toBufferedOutputStream(localFile), Topic("testTopic").withPartition(1))
    val parquetFormatWriter = new ParquetFormatWriter(blobStream)
    parquetFormatWriter.write(
      None,
      ArraySinkData(
        Seq(
          StringSinkData("batman"),
          StringSinkData("robin"),
          StringSinkData("alfred"),
        ),
      ),
      topic,
    ).left.value.getMessage should be("Schema-less data is not supported for Avro/Parquet")
  }

  "convert" should "throw an exception when trying to write map values" in {

    val mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA)

    val blobStream          = new BuildLocalOutputStream(toBufferedOutputStream(localFile), Topic("testTopic").withPartition(1))
    val parquetFormatWriter = new ParquetFormatWriter(blobStream)
    parquetFormatWriter.write(
      None,
      MapSinkData(
        Map(
          StringSinkData("batman") -> IntSinkData(1),
          StringSinkData("robin")  -> IntSinkData(2),
          StringSinkData("alfred") -> IntSinkData(3),
        ),
        Some(mapSchema),
      ),
      topic,
    ).left.value.getMessage should be("Avro schema must be a record.")
    parquetFormatWriter.complete() should be(Right(()))
  }

  // LZ4 and LZO need some extra native libs available on the environment and is out of scope
  // for getting this working.
  Parquet.availableCompressionCodecs.removedAll(Set(UNCOMPRESSED, LZ4, LZO, BROTLI)).keys.foreach {
    codec =>
      "convert" should s"compress output stream with $codec" in {
        val uncompressedBytes = {
          writeToParquetFile(UNCOMPRESSED.toCodec())
          localFileAsBytes(localFile)
        }
        val compressedBytes = {
          writeToParquetFile(codec.toCodec())
          localFileAsBytes(localFile)
        }
        val genericRecords = parquetFormatReader.read(compressedBytes)
        genericRecords.size should be(3)

        logger.info(
          "Uncompressed: {}, Compressed: {}, Smaller: {}",
          uncompressedBytes.length,
          compressedBytes.length,
          compressedBytes.length < uncompressedBytes.length,
        )

        // compression isn't always smaller, especially small samples
        compressedBytes.length shouldNot be(uncompressedBytes.length)
      }
  }

  private def writeToParquetFile(implicit compressionCodec: CompressionCodec) = {
    val blobStream = new BuildLocalOutputStream(toBufferedOutputStream(localFile), Topic("testTopic").withPartition(1))

    val parquetFormatWriter = new ParquetFormatWriter(blobStream)
    firstUsers.foreach(u => parquetFormatWriter.write(None, StructSinkData(u), topic) should be(Right(())))
    parquetFormatWriter.complete() should be(Right(()))
  }
}
