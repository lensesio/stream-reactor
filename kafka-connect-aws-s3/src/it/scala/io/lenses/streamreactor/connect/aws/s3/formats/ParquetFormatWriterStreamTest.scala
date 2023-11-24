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

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.utils.ITSampleSchemaAndData.firstUsers
import io.lenses.streamreactor.connect.cloud.common.utils.ITSampleSchemaAndData.users
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData.checkRecord
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData.topic
import io.lenses.streamreactor.connect.cloud.common.config.ParquetFormatSelection
import io.lenses.streamreactor.connect.cloud.common.formats.reader.ParquetFormatReader
import io.lenses.streamreactor.connect.cloud.common.formats.writer._
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.BROTLI
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.LZ4
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.LZO
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.UNCOMPRESSED
import io.lenses.streamreactor.connect.cloud.common.model.location.FileUtils.toBufferedOutputStream
import io.lenses.streamreactor.connect.cloud.common.stream.BuildLocalOutputStream
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

class ParquetFormatWriterStreamTest
    extends AnyFlatSpec
    with Matchers
    with S3ProxyContainerTest
    with EitherValues
    with LazyLogging {

  implicit val compressionCodec: CompressionCodec = UNCOMPRESSED.toCodec()

  val parquetFormatReader = new ParquetFormatReader()

  "convert" should "write byte output stream with json for a single record" in {

    implicit val compressionCodec: CompressionCodec = UNCOMPRESSED.toCodec()

    val blobStream          = new BuildLocalOutputStream(toBufferedOutputStream(localFile), Topic("testTopic").withPartition(1))
    val parquetFormatWriter = new ParquetFormatWriter(blobStream)
    parquetFormatWriter.write(MessageDetail(NullSinkData(None),
                                            StructSinkData(users.head),
                                            Map.empty,
                                            None,
                                            topic,
                                            1,
                                            Offset(1),
    ))
    parquetFormatWriter.getPointer should be(21)
    parquetFormatWriter.write(MessageDetail(NullSinkData(None),
                                            StructSinkData(users(1)),
                                            Map.empty,
                                            None,
                                            topic,
                                            1,
                                            Offset(2),
    ))
    parquetFormatWriter.getPointer should be(44)
    parquetFormatWriter.write(MessageDetail(NullSinkData(None),
                                            StructSinkData(users(2)),
                                            Map.empty,
                                            None,
                                            topic,
                                            1,
                                            Offset(3),
    ))
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
      MessageDetail(
        NullSinkData(None),
        ArraySinkData(
          Seq(
            "batman",
            "robin",
            "alfred",
          ).asJava,
        ),
        Map.empty,
        None,
        topic,
        1,
        Offset(1),
      ),
    ).left.value.getMessage should be("Schema-less data is not supported for Avro/Parquet")
  }

  "convert" should "throw an exception when trying to write map values" in {

    val mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA)

    val blobStream          = new BuildLocalOutputStream(toBufferedOutputStream(localFile), Topic("testTopic").withPartition(1))
    val parquetFormatWriter = new ParquetFormatWriter(blobStream)
    parquetFormatWriter.write(
      MessageDetail(
        NullSinkData(None),
        MapSinkData(
          Map(
            "batman" -> 1,
            "robin"  -> 2,
            "alfred" -> 3,
          ).asJava,
          Some(mapSchema),
        ),
        Map.empty,
        None,
        topic,
        1,
        Offset(1),
      ),
    ).left.value.getMessage should be("Avro schema must be a record.")
    parquetFormatWriter.complete() should be(Right(()))
  }

  // LZ4 and LZO need some extra native libs available on the environment and is out of scope
  // for getting this working.
  ParquetFormatSelection.availableCompressionCodecs.removedAll(Set(UNCOMPRESSED, LZ4, LZO, BROTLI)).keys.foreach {
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
    firstUsers.foreach(u =>
      parquetFormatWriter.write(MessageDetail(NullSinkData(None),
                                              StructSinkData(u),
                                              Map.empty,
                                              None,
                                              topic,
                                              1,
                                              Offset(1),
      )) should be(
        Right(()),
      ),
    )
    parquetFormatWriter.complete() should be(Right(()))
  }
}
