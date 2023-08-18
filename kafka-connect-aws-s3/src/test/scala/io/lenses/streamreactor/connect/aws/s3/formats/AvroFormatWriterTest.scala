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

import io.lenses.streamreactor.connect.aws.s3.formats.reader.AvroFormatReader
import io.lenses.streamreactor.connect.aws.s3.formats.writer._
import io.lenses.streamreactor.connect.aws.s3.model.CompressionCodec
import io.lenses.streamreactor.connect.aws.s3.model.Offset
import io.lenses.streamreactor.connect.aws.s3.model.CompressionCodecName.UNCOMPRESSED
import io.lenses.streamreactor.connect.aws.s3.stream.S3ByteArrayOutputStream
import io.lenses.streamreactor.connect.aws.s3.utils.SampleData
import io.lenses.streamreactor.connect.aws.s3.utils.SampleData._
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.ByteBuffer
import java.time.Instant
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

class AvroFormatWriterTest extends AnyFlatSpec with Matchers with EitherValues {
  private implicit val compressionCodec: CompressionCodec = UNCOMPRESSED.toCodec()

  private val arraySchema = SchemaBuilder.array(Schema.STRING_SCHEMA)

  private val avroFormatReader = new AvroFormatReader()

  "convert" should "write byte output stream with avro for a single record" in {

    val outputStream     = new S3ByteArrayOutputStream()
    val avroFormatWriter = new AvroFormatWriter(outputStream)
    avroFormatWriter.write(MessageDetail(NullSinkData(None),
                                         StructSinkData(SampleData.Users.head),
                                         Map.empty,
                                         Some(Instant.now()),
                                         topic,
                                         0,
                                         Offset(0),
    ))
    avroFormatWriter.complete()

    val genericRecords = avroFormatReader.read(outputStream.toByteArray)

    genericRecords.size should be(1)
    checkRecord(genericRecords.head, "sam", "mr", 100.43)
  }

  "convert" should "write byte output stream with avro for multiple records" in {

    val outputStream     = new S3ByteArrayOutputStream()
    val avroFormatWriter = new AvroFormatWriter(outputStream)
    SampleData.Users.take(3).foreach(u =>
      avroFormatWriter.write(MessageDetail(NullSinkData(None),
                                           StructSinkData(u),
                                           Map.empty,
                                           Some(Instant.now()),
                                           topic,
                                           0,
                                           Offset(0),
      )),
    )
    avroFormatWriter.complete()

    val genericRecords = avroFormatReader.read(outputStream.toByteArray)
    genericRecords.size should be(3)
  }

  "convert" should "write byte output stream with avro for a single primitive record" in {

    val outputStream     = new S3ByteArrayOutputStream()
    val avroFormatWriter = new AvroFormatWriter(outputStream)
    avroFormatWriter.write(
      MessageDetail(NullSinkData(None),
                    IntSinkData(100, Some(Schema.OPTIONAL_INT32_SCHEMA)),
                    Map.empty,
                    Some(Instant.now()),
                    topic,
                    0,
                    Offset(0),
      ),
    )
    avroFormatWriter.complete()

    val genericRecords = avroFormatReader.read(outputStream.toByteArray)

    genericRecords.size should be(1)
    genericRecords.head should be(100)
  }

  "convert" should "write byte output stream with avro for a multiple primitive records" in {

    val outputStream     = new S3ByteArrayOutputStream()
    val avroFormatWriter = new AvroFormatWriter(outputStream)
    avroFormatWriter.write(
      MessageDetail(NullSinkData(None),
                    IntSinkData(100, Some(Schema.OPTIONAL_INT32_SCHEMA)),
                    Map.empty,
                    Some(Instant.now()),
                    topic,
                    0,
                    Offset(0),
      ),
    )
    avroFormatWriter.write(
      MessageDetail(NullSinkData(None),
                    IntSinkData(200, Some(Schema.OPTIONAL_INT32_SCHEMA)),
                    Map.empty,
                    Some(Instant.now()),
                    topic,
                    0,
                    Offset(0),
      ),
    )
    avroFormatWriter.complete()

    val genericRecords = avroFormatReader.read(outputStream.toByteArray)

    genericRecords.size should be(2)
    genericRecords(0) should be(100)
    genericRecords(1) should be(200)
  }

  "convert" should "write byte output stream with avro for single array record" in {

    val outputStream     = new S3ByteArrayOutputStream()
    val avroFormatWriter = new AvroFormatWriter(outputStream)
    avroFormatWriter.write(
      MessageDetail(
        NullSinkData(None),
        ArraySinkData(
          Seq(
            "batman",
            "robin",
            "alfred",
          ).asJava,
          Some(arraySchema),
        ),
        Map.empty,
        Some(Instant.now()),
        topic,
        0,
        Offset(0),
      ),
    )
    avroFormatWriter.complete()

    val genericRecords = avroFormatReader.read(outputStream.toByteArray)
    genericRecords.size should be(1)

    checkArray(genericRecords.head.asInstanceOf[GenericData.Array[Utf8]], "batman", "robin", "alfred")

  }

  "convert" should "write byte output stream with avro for multiple array record" in {

    val outputStream     = new S3ByteArrayOutputStream()
    val avroFormatWriter = new AvroFormatWriter(outputStream)
    avroFormatWriter.write(
      MessageDetail(
        NullSinkData(None),
        ArraySinkData(
          Seq(
            "batman",
            "robin",
            "alfred",
          ).asJava,
          Some(arraySchema),
        ),
        Map.empty,
        Some(Instant.now()),
        topic,
        0,
        Offset(0),
      ),
    )
    avroFormatWriter.write(
      MessageDetail(
        NullSinkData(None),
        ArraySinkData(
          Seq(
            "superman",
            "lois lane",
          ).asJava,
          Some(arraySchema),
        ),
        Map.empty,
        Some(Instant.now()),
        topic,
        0,
        Offset(0),
      ),
    )
    avroFormatWriter.complete()

    val genericRecords = avroFormatReader.read(outputStream.toByteArray)
    genericRecords.size should be(2)

    checkArray(genericRecords(0).asInstanceOf[GenericData.Array[Utf8]], "batman", "robin", "alfred")
    checkArray(genericRecords(1).asInstanceOf[GenericData.Array[Utf8]], "superman", "lois lane")

  }

  "convert" should "throw an error when writing array without schema" in {

    val outputStream     = new S3ByteArrayOutputStream()
    val avroFormatWriter = new AvroFormatWriter(outputStream)
    val caught = avroFormatWriter.write(
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
        Some(Instant.now()),
        topic,
        0,
        Offset(0),
      ),
    )

    caught.left.value.getMessage should be("Schema-less data is not supported for Avro/Parquet")
  }

  "convert" should "write byte output stream with avro for multiple map records" in {
    val mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA)

    val outputStream     = new S3ByteArrayOutputStream()
    val avroFormatWriter = new AvroFormatWriter(outputStream)
    avroFormatWriter.write(
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
        Some(Instant.now()),
        topic,
        0,
        Offset(0),
      ),
    )
    avroFormatWriter.write(
      MessageDetail(
        NullSinkData(None),
        MapSinkData(
          Map(
            "superman"  -> 4,
            "lois lane" -> 5,
          ).asJava,
          Some(mapSchema),
        ),
        Map.empty,
        Some(Instant.now()),
        topic,
        0,
        Offset(0),
      ),
    )
    avroFormatWriter.complete()

    val genericRecords = avroFormatReader.read(outputStream.toByteArray)
    genericRecords.size should be(2)

    readFromStringKeyedMap(genericRecords, 0) should be(Map(
      "batman" -> 1,
      "robin"  -> 2,
      "alfred" -> 3,
    ))

    readFromStringKeyedMap(genericRecords, 1) should be(Map(
      "superman"  -> 4,
      "lois lane" -> 5,
    ))

  }

  "convert" should "write byte to avro" in {
    val byteSchema = SchemaBuilder.bytes().build()

    val outputStream     = new S3ByteArrayOutputStream()
    val avroFormatWriter = new AvroFormatWriter(outputStream)
    avroFormatWriter.write(
      MessageDetail(
        NullSinkData(None),
        ByteArraySinkData("Sausages".getBytes(), Some(byteSchema)),
        Map.empty,
        Some(Instant.now()),
        topic,
        0,
        Offset(0),
      ),
    )
    avroFormatWriter.write(
      MessageDetail(
        NullSinkData(None),
        ByteArraySinkData("Mash".getBytes(), Some(byteSchema)),
        Map.empty,
        Some(Instant.now()),
        topic,
        0,
        Offset(0),
      ),
    )
    avroFormatWriter.complete()

    val genericRecords = avroFormatReader.read(outputStream.toByteArray)
    genericRecords.size should be(2)

    genericRecords(0).asInstanceOf[ByteBuffer].array() should be("Sausages".getBytes())
    genericRecords(1).asInstanceOf[ByteBuffer].array() should be("Mash".getBytes())
  }

}
