
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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.NullNode
import io.lenses.streamreactor.connect.aws.s3.model.{ArraySinkData, MapSinkData, NullSinkData, StringSinkData, StructSinkData}
import io.lenses.streamreactor.connect.aws.s3.sink.utils.TestSampleSchemaAndData._
import io.lenses.streamreactor.connect.aws.s3.storage.S3ByteArrayOutputStream
import org.apache.kafka.connect.data.SchemaBuilder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JsonFormatWriterTest extends AnyFlatSpec with Matchers {


  "convert" should "write byteoutputstream with json for a single record" in {

    val outputStream = new S3ByteArrayOutputStream()
    val jsonFormatWriter = new JsonFormatWriter(() => outputStream)
    jsonFormatWriter.write(None, StructSinkData(users.head), topic)

    outputStream.toString should be(recordsAsJson.head + "\n")

  }

  "convert" should "write byteoutputstream with json for multiple records" in {

    val outputStream = new S3ByteArrayOutputStream()
    val jsonFormatWriter = new JsonFormatWriter(() => outputStream)
    users.foreach(e => jsonFormatWriter.write(None, StructSinkData(e), topic))

    outputStream.toString should be(recordsAsJson.mkString("\n"))

  }

  "convert" should "write primitive to json for a single record without schemas" in {

    val outputStream = new S3ByteArrayOutputStream()
    val jsonFormatWriter = new JsonFormatWriter(() => outputStream)
    jsonFormatWriter.write(None, StringSinkData("bees", None), topic)

    val objectMapper = new ObjectMapper()
    val tree = objectMapper.readTree(outputStream.toString())

    tree.textValue() should be("bees")
  }

  "convert" should "write primitive to json for a single record" in {

    val outputStream = new S3ByteArrayOutputStream()
    val jsonFormatWriter = new JsonFormatWriter(() => outputStream)
    jsonFormatWriter.write(None, StringSinkData("bees", Some(SchemaBuilder.string().build())), topic)

    val objectMapper = new ObjectMapper()
    val tree = objectMapper.readTree(outputStream.toString())

    tree.textValue() should be("bees")
  }


  "convert" should "write primitives to json for multiple records" in {

    val outputStream = new S3ByteArrayOutputStream()
    val jsonFormatWriter = new JsonFormatWriter(() => outputStream)
    jsonFormatWriter.write(None, StringSinkData("bees", Some(SchemaBuilder.string().build())), topic)
    jsonFormatWriter.write(None, StringSinkData("wasps", Some(SchemaBuilder.string().build())), topic)

    val lines = outputStream.toString().split(System.lineSeparator())
    val treeLine1 = new ObjectMapper().readTree(lines(0))
    treeLine1.textValue() should be("bees")

    val treeLine2 = new ObjectMapper().readTree(lines(1))
    treeLine2.textValue() should be("wasps")
  }


  "convert" should "write array to json for multiple records" in {

    val outputStream = new S3ByteArrayOutputStream()
    val jsonFormatWriter = new JsonFormatWriter(() => outputStream)
    jsonFormatWriter.write(None, ArraySinkData(
      Seq(
        StringSinkData("bees"),
        StringSinkData("wasps")
      )
    ), topic)

    val lines = outputStream.toString().split(System.lineSeparator())
    val treeLine1 = new ObjectMapper().readTree(lines(0))
    treeLine1.get(0).textValue() should be("bees")
    treeLine1.get(1).textValue() should be("wasps")
  }


  "convert" should "write map to json" in {

    val outputStream = new S3ByteArrayOutputStream()
    val jsonFormatWriter = new JsonFormatWriter(() => outputStream)
    jsonFormatWriter.write(None, MapSinkData(
      Map(
        StringSinkData("bees") -> StringSinkData("sting when scared"),
        StringSinkData("wasps") -> StringSinkData("sting for fun")
      )
    ), topic)

    val lines = outputStream.toString().split(System.lineSeparator())
    val treeLine1 = new ObjectMapper().readTree(lines(0))
    treeLine1.get("bees").textValue() should be("sting when scared")
    treeLine1.get("wasps").textValue() should be("sting for fun")
  }

  "convert" should "write maps containing nulls as null in json" in {

    val outputStream = new S3ByteArrayOutputStream()
    val jsonFormatWriter = new JsonFormatWriter(() => outputStream)
    jsonFormatWriter.write(None, MapSinkData(
      Map(
        StringSinkData("bees") -> StringSinkData("sting when scared"),
        StringSinkData("wasps") -> NullSinkData()
      )
    ), topic)

    val lines = outputStream.toString().split(System.lineSeparator())
    val treeLine1 = new ObjectMapper().readTree(lines(0))
    treeLine1.get("bees").textValue() should be("sting when scared")
    treeLine1.get("wasps") shouldBe a [NullNode]
  }
}
