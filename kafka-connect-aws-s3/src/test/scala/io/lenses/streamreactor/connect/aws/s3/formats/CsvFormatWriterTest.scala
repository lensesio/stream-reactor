
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

import java.io.StringReader
import au.com.bytecode.opencsv.CSVReader
import io.lenses.streamreactor.connect.aws.s3.model.StructSinkData
import io.lenses.streamreactor.connect.aws.s3.sink.extractors.ExtractorError
import io.lenses.streamreactor.connect.aws.s3.sink.extractors.ExtractorErrorType.UnexpectedType
import io.lenses.streamreactor.connect.aws.s3.sink.utils.TestSampleSchemaAndData._
import io.lenses.streamreactor.connect.aws.s3.storage.S3ByteArrayOutputStream
import org.apache.kafka.connect.data.Schema.STRING_SCHEMA
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.scalatest.Assertions
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class CsvFormatWriterTest extends AnyFlatSpec with Matchers with Assertions {

  "convert" should "write byte output stream with csv for a single record" in {

    val outputStream = new S3ByteArrayOutputStream()
    val formatWriter = new CsvFormatWriter(() => outputStream, true)
    formatWriter.write(None, StructSinkData(users.head), topic)

    val reader = new StringReader(new String(outputStream.toByteArray))

    val csvReader = new CSVReader(reader)

    csvReader.readNext() should be(Array("name", "title", "salary"))
    csvReader.readNext() should be(Array("sam", "mr", "100.43"))
    csvReader.readNext() should be(null)

    csvReader.close()
    reader.close()
  }

  "convert" should "write byte output stream with csv for multiple records" in {

    val outputStream = new S3ByteArrayOutputStream()
    val formatWriter = new CsvFormatWriter(() => outputStream, true)
    firstUsers.foreach(
      e =>
        formatWriter.write(
          None,
          StructSinkData(e),
          topic
        )
    )

    val reader = new StringReader(new String(outputStream.toByteArray))
    val csvReader = new CSVReader(reader)

    csvReader.readNext() should be(Array("name", "title", "salary"))
    csvReader.readNext() should be(Array("sam", "mr", "100.43"))
    csvReader.readNext() should be(Array("laura", "ms", "429.06"))
    csvReader.readNext() should be(Array("tom", "", "395.44"))
    csvReader.readNext() should be(null)

    csvReader.close()
    reader.close()

  }

  "convert" should "allow all primitive types" in {

    val schema: Schema = SchemaBuilder.struct()
      .field("myString", SchemaBuilder.string().build())
      .field("myBool", SchemaBuilder.bool().build())
      .field("myBytes", SchemaBuilder.bytes().build())
      .field("myFloat32", SchemaBuilder.float32().build())
      .field("myFloat64", SchemaBuilder.float64().build())
      .field("myInt8", SchemaBuilder.int8().build())
      .field("myInt16", SchemaBuilder.int16().build())
      .field("myInt32", SchemaBuilder.int32().build())
      .field("myInt64", SchemaBuilder.int64().build())
      .build()

    val struct = new Struct(schema)
      .put("myString", "testString")
      .put("myBool", true)
      .put("myBytes", "testBytes".getBytes)
      .put("myFloat32", 32.0.toFloat)
      .put("myFloat64", 64.02)
      .put("myInt8", 8.asInstanceOf[Byte])
      .put("myInt16", 16.toShort)
      .put("myInt32", 32)
      .put("myInt64", 64.toLong)


    val outputStream = new S3ByteArrayOutputStream()
    val formatWriter = new CsvFormatWriter(() => outputStream, true)
    formatWriter.write(None, StructSinkData(struct), topic)

    val reader = new StringReader(new String(outputStream.toByteArray))

    val csvReader = new CSVReader(reader)

    csvReader.readNext() should be(Array("myString", "myBool", "myBytes", "myFloat32", "myFloat64", "myInt8", "myInt16", "myInt32", "myInt64"))
    csvReader.readNext() should be(Array("testString", "true", "testBytes", "32.0", "64.02", "8", "16", "32", "64"))
    csvReader.readNext() should be(null)

    csvReader.close()
    reader.close()
  }

  "convert" should "not allow complex array types" in {

    val schema: Schema = SchemaBuilder.struct()
      .field("myStringArray", SchemaBuilder.array(STRING_SCHEMA).build())
      .build()

    val struct = new Struct(schema)
      .put("myStringArray", List("cheese", "biscuits").asJava)

    val outputStream = new S3ByteArrayOutputStream()
    val formatWriter = new CsvFormatWriter(() => outputStream, true)

    val caught = intercept[ExtractorError] {
      formatWriter.write(None, StructSinkData(struct), topic)
    }
    formatWriter.close()
    caught.extractorErrorType should be(UnexpectedType)
  }

  "convert" should "not allow complex struct types" in {

    val insideSchema: Schema = SchemaBuilder.struct().field("myStringStruct", STRING_SCHEMA).build()
    val envelopingSchema: Schema = SchemaBuilder.struct()
      .field("myEnvelopingStruct", insideSchema)
      .build()

    val struct = new Struct(envelopingSchema)
      .put("myEnvelopingStruct",
        new Struct(insideSchema)
          .put("myStringStruct", "myStringFieldValue")
      )

    val outputStream = new S3ByteArrayOutputStream()
    val formatWriter = new CsvFormatWriter(() => outputStream, true)

    val caught = intercept[ExtractorError] {
      formatWriter.write(None, StructSinkData(struct), topic)
    }
    formatWriter.close()
    caught.extractorErrorType should be(UnexpectedType)
  }
}
