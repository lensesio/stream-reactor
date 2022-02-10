
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

import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.model.location.FileUtils.toBufferedOutputStream
import io.lenses.streamreactor.connect.aws.s3.sink.utils.S3TestConfig
import io.lenses.streamreactor.connect.aws.s3.sink.utils.TestSampleSchemaAndData._
import io.lenses.streamreactor.connect.aws.s3.stream.BuildLocalOutputStream
import org.apache.kafka.connect.data.{Schema, SchemaBuilder}
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ParquetFormatWriterStreamTest extends AnyFlatSpec with Matchers with S3TestConfig with EitherValues {
  import helper._

  val parquetFormatReader = new ParquetFormatReader()

  "convert" should "write byte output stream with json for a single record" in {
    val blobStream = new BuildLocalOutputStream( toBufferedOutputStream(localFile), Topic("testTopic").withPartition(1))
    val parquetFormatWriter = new ParquetFormatWriter(() => blobStream)
    parquetFormatWriter.write(None, StructSinkData(users.head), topic)
    parquetFormatWriter.getPointer should be (21)
    parquetFormatWriter.write(None, StructSinkData(users(1)), topic)
    parquetFormatWriter.getPointer should be (44)
    parquetFormatWriter.write(None, StructSinkData(users(2)), topic)
    parquetFormatWriter.getPointer should be (59)
    parquetFormatWriter.complete() should be (Right(()))

    val bytes = localFileAsBytes(localFile)

    val genericRecords = parquetFormatReader.read(bytes)
    genericRecords.size should be(3)
    checkRecord(genericRecords.head, "sam", "mr", 100.43)

  }

  "convert" should "write byte output stream with json for multiple records" in {
    val blobStream = new BuildLocalOutputStream(toBufferedOutputStream(localFile), Topic("testTopic").withPartition(1))
    val parquetFormatWriter = new ParquetFormatWriter(() => blobStream)
    firstUsers.foreach(e => parquetFormatWriter.write(None, StructSinkData(e), topic))
    parquetFormatWriter.complete() should be (Right(()))

    val bytes = localFileAsBytes(localFile)
    val genericRecords = parquetFormatReader.read(bytes)
    genericRecords.size should be(3)

  }

  "convert" should "throw an error when writing array without schema" in {

    val blobStream = new BuildLocalOutputStream(toBufferedOutputStream(localFile), Topic("testTopic").withPartition(1))
    val parquetFormatWriter = new ParquetFormatWriter(() => blobStream)
      parquetFormatWriter.write(
        None,
        ArraySinkData(
          Array(
            StringSinkData("batman"),
            StringSinkData("robin"),
            StringSinkData("alfred")
          )),
        topic).left.value.getMessage should be("Schema-less data is not supported for Avro/Parquet")
  }

  "convert" should "throw an exception when trying to write map values" in {
    val mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA)

    val blobStream = new BuildLocalOutputStream(toBufferedOutputStream(localFile), Topic("testTopic").withPartition(1))
    val parquetFormatWriter = new ParquetFormatWriter(() => blobStream)
      parquetFormatWriter.write(
        None,
        MapSinkData(
          Map(
            StringSinkData("batman") -> IntSinkData(1),
            StringSinkData("robin") -> IntSinkData(2),
            StringSinkData("alfred") -> IntSinkData(3)
          ), Some(mapSchema)),
        topic).left.value.getMessage should be("Avro schema must be a record.")
    parquetFormatWriter.complete() should be (Right(()))
  }
}
