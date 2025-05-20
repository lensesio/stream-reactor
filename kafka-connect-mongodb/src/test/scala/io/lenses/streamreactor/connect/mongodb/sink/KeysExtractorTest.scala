/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.mongodb.sink

import java.util
import io.lenses.streamreactor.connect.mongodb.Json
import com.sksamuel.avro4s.RecordFormat
import io.confluent.connect.avro.AvroData
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.data.Struct
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.ListSet
import scala.jdk.CollectionConverters.MapHasAsJava

class KeysExtractorTest extends AnyWordSpec with Matchers {
  private val avroData = new AvroData(4)

  case class WithNested(id: Int, nested: SomeTest)
  case class SomeTest(name: String, value: Double, flags: Seq[Int], map: Map[String, String])

  case class Thing(A: Int, B: String, C: CThing)
  case class CThing(M: String, N: NThing)
  case class NThing(X: Int, Y: Int)

  "KeysExtractor" should {
    "extract keys from JSON" in {
      val json   = scala.io.Source.fromFile(getClass.getResource(s"/transaction1.json").toURI.getPath).mkString
      val jvalue = Json.parseJson(json)

      val actual = KeysExtractor.fromJson(jvalue, ListSet("lock_time", "rbf"))
      actual shouldBe List("lock_time" -> 9223372036854775807L, "rbf" -> true)
    }

    "extract embedded keys out of JSON" in {
      val jvalue = Json.parseJson("""{"A": 0, "B": "0", "C": {"M": "1000", "N": {"X": 10, "Y": 100} } }""")
      val keys   = ListSet("B", "C.M", "C.N.X")
      // SCALA 2.12 WARNING: If you upgrade to 2.12 and this test fails,
      // you need to remove the "reverse()" calls in KeysExtractor.scala:
      KeysExtractor.fromJson(jvalue, keys) shouldBe
        List("B" -> "0", "M" -> "1000", "X" -> 10)
    }

    "throw exception when extracting the keys from JSON" in {
      val json   = scala.io.Source.fromFile(getClass.getResource(s"/transaction1.json").toURI.getPath).mkString
      val jvalue = Json.parseJson(json)
      intercept[ConfigException] {
        val _ = KeysExtractor.fromJson(jvalue, Set("inputs"))
      }
    }

    "extract keys from a Map" in {
      val actual =
        KeysExtractor.fromMap(Map[String, Any]("key1" -> 12, "key2" -> 10L, "key3" -> "tripple").asJava,
                              Set("key1", "key3"),
        )
      actual shouldBe Set("key1" -> 12, "key3" -> "tripple")
    }

    "extract embedded keys out of a Map (including Dates)" in {
      val actual = KeysExtractor.fromMap(
        Map[String, Any](
          "A" -> 0,
          "B" -> "0",
          "C" -> Map[String, Any]("M" -> "1000",
                                  "N" -> Map[String, Any]("X" -> new java.util.Date(10L), "Y" -> 100).asJava,
          ).asJava,
        ).asJava,
        ListSet("B", "C.M", "C.N.X"),
      )
      // SCALA 2.12 WARNING: If you upgrade to 2.12 and this test fails,
      // you need to remove the "reverse()" calls in KeysExtractor.scala:
      actual shouldBe ListSet("B" -> "0", "M" -> "1000", "X" -> new java.util.Date(10L))
    }

    "extract keys from a Map should throw an exception if the key is another map" in {
      intercept[ConfigException] {
        KeysExtractor.fromMap(Map[String, Any]("key1" -> 12, "key2" -> 10L, "key3" -> Map.empty[String, String]).asJava,
                              Set("key1", "key3"),
        )
      }
    }

    "extract keys from a Map should throw an exception if the key is an array" in {
      intercept[ConfigException] {
        KeysExtractor.fromMap(
          Map[String, Any]("key1" -> 12, "key2" -> 10L, "key3" -> new util.ArrayList[String]).asJava,
          Set("key1", "key3"),
        )
      }
    }

    "extract from a struct" in {
      val format = RecordFormat[SomeTest]
      val avro   = format.to(SomeTest("abc", 12.5, Seq.empty, Map.empty))
      val struct = avroData.toConnectData(avro.getSchema, avro)
      KeysExtractor.fromStruct(struct.value().asInstanceOf[Struct], Set("name")) shouldBe
        Set("name" -> "abc")
    }

    "extract embedded keys out of a struct" in {
      val format = RecordFormat[Thing]
      val avro   = format.to(Thing(0, "0", CThing("1000", NThing(10, 100))))
      val struct = avroData.toConnectData(avro.getSchema, avro)
      // SCALA 2.12 WARNING: If you upgrade to 2.12 and this test fails,
      // you need to remove the "reverse()" calls in KeysExtractor.scala:
      KeysExtractor.fromStruct(
        struct.value().asInstanceOf[Struct],
        ListSet("B", "C.M", "C.N.X"),
      ) shouldBe
        ListSet("B" -> "0", "M" -> "1000", "X" -> 10)
    }

    "extract from a struct should throw an exception if a key is an array" in {
      val format = RecordFormat[SomeTest]
      val avro   = format.to(SomeTest("abc", 12.5, Seq.empty, Map.empty))
      intercept[ConfigException] {
        val struct = avroData.toConnectData(avro.getSchema, avro)
        KeysExtractor.fromStruct(struct.value().asInstanceOf[Struct], Set("flags"))
      }
    }

    "extract from a struct should throw an exception if a key is a map" in {
      val format = RecordFormat[SomeTest]
      val avro   = format.to(SomeTest("abc", 12.5, Seq.empty, Map.empty))
      intercept[ConfigException] {
        val struct = avroData.toConnectData(avro.getSchema, avro)
        KeysExtractor.fromStruct(struct.value().asInstanceOf[Struct], Set("map"))
      }
    }

    "extract from a struct should throw an exception if a key is a struct" in {
      val format = RecordFormat[WithNested]
      val avro   = format.to(WithNested(1, SomeTest("abc", 12.5, Seq.empty, Map.empty)))
      intercept[ConfigException] {
        val struct = avroData.toConnectData(avro.getSchema, avro)
        KeysExtractor.fromStruct(struct.value().asInstanceOf[Struct], Set("nested"))
      }
    }
  }
}
