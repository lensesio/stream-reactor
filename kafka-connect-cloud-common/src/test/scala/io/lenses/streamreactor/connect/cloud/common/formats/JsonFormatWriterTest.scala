/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.formats

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.NullNode
import io.lenses.streamreactor.connect.cloud.common.formats.writer._
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.stream.CloudByteArrayOutputStream
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData.recordsAsJson
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData.topic
import org.apache.kafka.connect.data.SchemaBuilder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

class JsonFormatWriterTest extends AnyFlatSpec with Matchers {

  "convert" should "write byte output stream with json for a single record" in {

    val outputStream     = new CloudByteArrayOutputStream()
    val jsonFormatWriter = new JsonFormatWriter(outputStream)
    jsonFormatWriter.write(MessageDetail(NullSinkData(None),
                                         StructSinkData(SampleData.Users.head),
                                         Map.empty,
                                         Some(Instant.now()),
                                         topic,
                                         0,
                                         Offset(0),
    ))

    outputStream.toString should be(recordsAsJson.head + "\n")

  }

  "convert" should "write byte output stream with json for multiple records" in {

    val outputStream     = new CloudByteArrayOutputStream()
    val jsonFormatWriter = new JsonFormatWriter(outputStream)
    SampleData.Users.take(3).foreach(e =>
      jsonFormatWriter.write(
        MessageDetail(NullSinkData(None), StructSinkData(e), Map.empty, Some(Instant.now()), topic, 0, Offset(0)),
      ),
    )

    outputStream.toString should be(recordsAsJson.mkString("\n"))

  }

  "convert" should "write primitive to json for a single record without schemas" in {

    val outputStream     = new CloudByteArrayOutputStream()
    val jsonFormatWriter = new JsonFormatWriter(outputStream)
    jsonFormatWriter.write(MessageDetail(NullSinkData(None),
                                         StringSinkData("bees", None),
                                         Map.empty,
                                         Some(Instant.now()),
                                         topic,
                                         0,
                                         Offset(0),
    ))

    val objectMapper = new ObjectMapper()
    val tree         = objectMapper.readTree(outputStream.toString())

    tree.textValue() should be("bees")
  }

  "convert" should "write primitive with new line characters to single line" in {

    val outputStream     = new CloudByteArrayOutputStream()
    val jsonFormatWriter = new JsonFormatWriter(outputStream)
    jsonFormatWriter.write(MessageDetail(NullSinkData(None),
                                         StringSinkData("apple\nbucket", None),
                                         Map.empty,
                                         Some(Instant.now()),
                                         topic,
                                         0,
                                         Offset(0),
    ))

    val objectMapper = new ObjectMapper()
    val tree         = objectMapper.readTree(outputStream.toString())

    tree.textValue() should be("apple\\nbucket")
  }

  "convert" should "write primitive to json for a single record" in {

    val outputStream     = new CloudByteArrayOutputStream()
    val jsonFormatWriter = new JsonFormatWriter(outputStream)
    jsonFormatWriter.write(
      MessageDetail(
        NullSinkData(None),
        StringSinkData("bees", Some(SchemaBuilder.string().build())),
        Map.empty,
        Some(Instant.now()),
        topic,
        0,
        Offset(0),
      ),
    )

    val objectMapper = new ObjectMapper()
    val tree         = objectMapper.readTree(outputStream.toString())

    tree.textValue() should be("bees")
  }

  "convert" should "write primitives to json for multiple records" in {

    val outputStream     = new CloudByteArrayOutputStream()
    val jsonFormatWriter = new JsonFormatWriter(outputStream)
    jsonFormatWriter.write(
      MessageDetail(
        NullSinkData(None),
        StringSinkData("bees", Some(SchemaBuilder.string().build())),
        Map.empty,
        Some(Instant.now()),
        topic,
        0,
        Offset(1),
      ),
    )
    jsonFormatWriter.write(
      MessageDetail(
        NullSinkData(None),
        StringSinkData("wasps", Some(SchemaBuilder.string().build())),
        Map.empty,
        Some(Instant.now()),
        topic,
        0,
        Offset(2),
      ),
    )

    val lines     = outputStream.toString().split(System.lineSeparator())
    val treeLine1 = new ObjectMapper().readTree(lines(0))
    treeLine1.textValue() should be("bees")

    val treeLine2 = new ObjectMapper().readTree(lines(1))
    treeLine2.textValue() should be("wasps")
  }

  "convert" should "write array to json for multiple records" in {

    val outputStream     = new CloudByteArrayOutputStream()
    val jsonFormatWriter = new JsonFormatWriter(outputStream)
    jsonFormatWriter.write(
      MessageDetail(NullSinkData(None),
                    ArraySinkData(
                      Seq(
                        "bees",
                        "wasps",
                      ).asJava,
                    ),
                    Map.empty,
                    Some(Instant.now()),
                    topic,
                    0,
                    Offset(0),
      ),
    )

    val lines     = outputStream.toString().split(System.lineSeparator())
    val treeLine1 = new ObjectMapper().readTree(lines(0))
    treeLine1.get(0).textValue() should be("bees")
    treeLine1.get(1).textValue() should be("wasps")
  }

  "convert" should "write map to json" in {

    val outputStream     = new CloudByteArrayOutputStream()
    val jsonFormatWriter = new JsonFormatWriter(outputStream)
    jsonFormatWriter.write(
      MessageDetail(
        NullSinkData(None),
        MapSinkData(
          Map(
            "bees"  -> "sting when scared",
            "wasps" -> "sting for fun",
          ).asJava,
        ),
        Map.empty,
        Some(Instant.now()),
        topic,
        0,
        Offset(0),
      ),
    )

    val lines     = outputStream.toString().split(System.lineSeparator())
    val treeLine1 = new ObjectMapper().readTree(lines(0))
    treeLine1.get("bees").textValue() should be("sting when scared")
    treeLine1.get("wasps").textValue() should be("sting for fun")
  }

  "convert" should "write maps containing nulls as null in json" in {

    val outputStream     = new CloudByteArrayOutputStream()
    val jsonFormatWriter = new JsonFormatWriter(outputStream)
    jsonFormatWriter.write(
      MessageDetail(
        NullSinkData(None),
        MapSinkData(
          Map(
            "bees"  -> "sting when scared",
            "wasps" -> null,
          ).asJava,
          Some(SchemaBuilder.map(SchemaBuilder.string().build(), SchemaBuilder.string().optional().build()).build()),
        ),
        Map.empty,
        Some(Instant.now()),
        topic,
        0,
        Offset(0),
      ),
    )

    val lines     = outputStream.toString().split(System.lineSeparator())
    val treeLine1 = new ObjectMapper().readTree(lines(0))
    treeLine1.get("bees").textValue() should be("sting when scared")
    treeLine1.get("wasps") shouldBe a[NullNode]
  }
}
