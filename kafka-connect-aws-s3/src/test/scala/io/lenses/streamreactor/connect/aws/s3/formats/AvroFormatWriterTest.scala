
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
import io.lenses.streamreactor.connect.aws.s3.sink.utils.TestSampleSchemaAndData._
import io.lenses.streamreactor.connect.aws.s3.storage.S3ByteArrayOutputStream
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.apache.kafka.connect.data.{Schema, SchemaBuilder}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AvroFormatWriterTest extends AnyFlatSpec with Matchers {

  private val arraySchema = SchemaBuilder.array(Schema.STRING_SCHEMA)

  private val avroFormatReader = new AvroFormatReader()

  "convert" should "write byteoutputstream with avro for a single record" in {

    val outputStream = new S3ByteArrayOutputStream()
    val avroFormatWriter = new AvroFormatWriter(() => outputStream)
    avroFormatWriter.write(None, StructSinkData(users(0)), topic)
    avroFormatWriter.close()

    val genericRecords = avroFormatReader.read(outputStream.toByteArray)

    genericRecords.size should be(1)
    checkRecord(genericRecords(0), "sam", "mr", 100.43)
  }

  "convert" should "write byteoutputstream with avro for multiple records" in {

    val outputStream = new S3ByteArrayOutputStream()
    val avroFormatWriter = new AvroFormatWriter(() => outputStream)
    users.foreach(u => avroFormatWriter.write(None, StructSinkData(u), topic))
    avroFormatWriter.close()

    val genericRecords = avroFormatReader.read(outputStream.toByteArray)
    genericRecords.size should be(3)
  }


  "convert" should "write byteoutputstream with avro for a single primitive record" in {

    val outputStream = new S3ByteArrayOutputStream()
    val avroFormatWriter = new AvroFormatWriter(() => outputStream)
    avroFormatWriter.write(None, IntSinkData(100, Some( Schema.OPTIONAL_INT32_SCHEMA)), topic)
    avroFormatWriter.close()

    val genericRecords = avroFormatReader.read(outputStream.toByteArray)

    genericRecords.size should be(1)
    genericRecords(0) should be (100)
  }

  "convert" should "write byteoutputstream with avro for a multiple primitive records" in {

    val outputStream = new S3ByteArrayOutputStream()
    val avroFormatWriter = new AvroFormatWriter(() => outputStream)
    avroFormatWriter.write(None, IntSinkData(100, Some( Schema.OPTIONAL_INT32_SCHEMA)), topic)
    avroFormatWriter.write(None, IntSinkData(200, Some( Schema.OPTIONAL_INT32_SCHEMA)), topic)
    avroFormatWriter.close()

    val genericRecords = avroFormatReader.read(outputStream.toByteArray)

    genericRecords.size should be(2)
    genericRecords(0) should be (100)
    genericRecords(1) should be (200)
  }


  "convert" should "write byteoutputstream with avro for single array record" in {

    val outputStream = new S3ByteArrayOutputStream()
    val avroFormatWriter = new AvroFormatWriter(() => outputStream)
    avroFormatWriter.write(
      None,
      ArraySinkData(
        Array(
          StringSinkData("batman"),
          StringSinkData("robin"),
          StringSinkData("alfred")
        ), Some(arraySchema)),
      topic)
    avroFormatWriter.close()

    val genericRecords = avroFormatReader.read(outputStream.toByteArray)
    genericRecords.size should be(1)

    checkArray(genericRecords(0).asInstanceOf[GenericData.Array[Utf8]], "batman", "robin", "alfred")

  }

  "convert" should "write byteoutputstream with avro for multiple array record" in {

    val outputStream = new S3ByteArrayOutputStream()
    val avroFormatWriter = new AvroFormatWriter(() => outputStream)
    avroFormatWriter.write(
      None,
      ArraySinkData(
        Array(
          StringSinkData("batman"),
          StringSinkData("robin"),
          StringSinkData("alfred")
        ), Some(arraySchema)),
      topic)
    avroFormatWriter.write(
      None,
      ArraySinkData(
        Array(
          StringSinkData("superman"),
          StringSinkData("lois lane")
        ), Some(arraySchema)),
      topic)
    avroFormatWriter.close()

    val genericRecords = avroFormatReader.read(outputStream.toByteArray)
    genericRecords.size should be(2)

    checkArray(genericRecords(0).asInstanceOf[GenericData.Array[Utf8]], "batman", "robin", "alfred")
    checkArray(genericRecords(1).asInstanceOf[GenericData.Array[Utf8]], "superman", "lois lane")

  }
}
