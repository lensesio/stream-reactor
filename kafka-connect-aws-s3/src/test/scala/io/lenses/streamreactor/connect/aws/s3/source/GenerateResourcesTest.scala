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
package io.lenses.streamreactor.connect.aws.s3.source

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.formats.bytes.BytesWriteMode.KeyAndValueWithSizes
import io.lenses.streamreactor.connect.aws.s3.formats.writer._
import io.lenses.streamreactor.connect.aws.s3.model.CompressionCodec
import io.lenses.streamreactor.connect.aws.s3.model.CompressionCodecName.UNCOMPRESSED
import io.lenses.streamreactor.connect.aws.s3.stream.S3ByteArrayOutputStream
import io.lenses.streamreactor.connect.aws.s3.stream.S3OutputStream
import io.lenses.streamreactor.connect.aws.s3.utils.TestSampleSchemaAndData.schema
import io.lenses.streamreactor.connect.aws.s3.utils.TestSampleSchemaAndData.topic
import org.apache.commons.io.FileUtils
import org.apache.kafka.connect.data.Struct
import org.scalacheck.Gen
import org.scalacheck.Gen.Choose.chooseDouble
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

class GenerateResourcesTest extends AnyFlatSpec with Matchers with LazyLogging {
  private implicit val compressionCodec: CompressionCodec = UNCOMPRESSED.toCodec()

  private val temporaryDirName: String = UUID.randomUUID().toString

  private val numberOfFiles   = 5
  private val numberOfRecords = 200

  private val avroWriterFn:         S3OutputStream => S3FormatWriter = new AvroFormatWriter(_)
  private val jsonWriterFn:         S3OutputStream => S3FormatWriter = new JsonFormatWriter(_)
  private val parquetWriterFn:      S3OutputStream => S3FormatWriter = new ParquetFormatWriter(_)
  private val csvHeadersWriterFn:   S3OutputStream => S3FormatWriter = new CsvFormatWriter(_, true)
  private val csvNoHeadersWriterFn: S3OutputStream => S3FormatWriter = new CsvFormatWriter(_, false)
  private val bytesKeyValueFn:      S3OutputStream => S3FormatWriter = new BytesFormatWriter(_, KeyAndValueWithSizes)

  private val writerClasses = Map(
    "avro"            -> avroWriterFn,
    "json"            -> jsonWriterFn,
    "parquet"         -> parquetWriterFn,
    "csv_withheaders" -> csvHeadersWriterFn,
    "csv"             -> csvNoHeadersWriterFn,
  )

  private val byteWriterClasses = Map(
    "bytes_keyandvaluewithsizes" -> bytesKeyValueFn,
  )

  def userGen: Gen[Struct] =
    for {
      name   <- Gen.alphaStr
      title  <- Gen.alphaStr
      salary <- Gen.choose(0.00, 1000.00)(chooseDouble)

    } yield new Struct(schema).put("name", name).put("title", title).put("salary", salary)

  /**
    * For AVRO, Parquet and Json writes 5 files, each with 200 records to a temporary directory
    */
  "generateResources" should "generate temporary test resources" ignore {

    val dir = s"${FileUtils.getTempDirectoryPath}/$temporaryDirName"
    val containerFile: File = new File(dir)

    if (!containerFile.exists()) {
      containerFile.mkdir()

      writerClasses.foreach {
        case (format, writerClass) =>
          1 to numberOfFiles foreach {
            fileNum =>
              val outputStream = new S3ByteArrayOutputStream
              val writer: S3FormatWriter = writerClass(outputStream)
              1 to numberOfRecords foreach { _ => writer.write(None, StructSinkData(userGen.sample.get), topic) }
              writer.complete() // TODO: FIX

              val dataFile = new File(s"$dir/$format/$fileNum.$format")
              logger.info(s"Writing $format file ${dataFile.getAbsolutePath}")
              FileUtils.writeByteArrayToFile(dataFile, outputStream.toByteArray)
          }
      }
    }

  }

  /**
    * Generates temporary resources for byte types
    */
  "generateResources" should "generate temporary test resources for bytes" ignore {

    val dir = s"${FileUtils.getTempDirectoryPath}/$temporaryDirName"
    val containerFile: File = new File(dir)

    if (!containerFile.exists()) {
      containerFile.mkdir()

      byteWriterClasses.foreach {
        case (format, writerClass) =>
          1 to numberOfFiles foreach {
            fileNum =>
              val outputStream = new S3ByteArrayOutputStream
              val writer: S3FormatWriter = writerClass(outputStream)
              1 to numberOfRecords foreach { _ =>
                writer.write(Some(ByteArraySinkData("myKey".getBytes)), ByteArraySinkData("somestring".getBytes), topic)
              }
              writer.complete() // TODO: FIX

              val dataFile = new File(s"$dir/$format/$fileNum.$format")
              logger.info(s"Writing $format file ${dataFile.getAbsolutePath}")
              FileUtils.writeByteArrayToFile(dataFile, outputStream.toByteArray)
          }
      }
    }

  }

}
