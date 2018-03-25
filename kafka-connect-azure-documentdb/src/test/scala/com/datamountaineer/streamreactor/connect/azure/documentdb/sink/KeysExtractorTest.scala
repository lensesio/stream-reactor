/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.azure.documentdb.sink

import java.util

import com.datamountaineer.streamreactor.connect.azure.documentdb.Json
import com.sksamuel.avro4s.RecordFormat
import io.confluent.connect.avro.AvroData
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.data.Struct
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConversions._

class KeysExtractorTest extends WordSpec with Matchers {
  private val avroData = new AvroData(4)

  case class WithNested(id: Int, nested: SomeTest)

  case class SomeTest(name: String, value: Double, flags: Seq[Int], map: Map[String, String])

  "KeysExtractor" should {
    "extract keys from JSON" in {
      val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction1.json").toURI.getPath).mkString
      val jvalue = Json.parseJson(json)

      val actual = KeysExtractor.fromJson(jvalue, Set("lock_time", "rbf"))
      actual shouldBe List("lock_time" -> 9223372036854775807L, "rbf" -> true)
    }

    "throw exception when extracting the keys from JSON" in {
      val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction1.json").toURI.getPath).mkString
      val jvalue = Json.parseJson(json)
      intercept[ConfigException] {
        val actual = KeysExtractor.fromJson(jvalue, Set("inputs"))
      }
    }


    "extract keys from a Map" in {
      val actual = KeysExtractor.fromMap(Map("key1" -> 12, "key2" -> 10L, "key3" -> "tripple"), Set("key1", "key3"))
      actual shouldBe Set("key1" -> 12, "key3" -> "tripple")
    }

    "extract keys from a Map should throw an exception if the key is another map" in {
      intercept[ConfigException] {
        KeysExtractor.fromMap(Map("key1" -> 12, "key2" -> 10L, "key3" -> Map.empty[String, String]), Set("key1", "key3"))
      }
    }

    "extract keys from a Map should throw an exception if the key is an array" in {
      intercept[ConfigException] {
        KeysExtractor.fromMap(Map("key1" -> 12, "key2" -> 10L, "key3" -> new util.ArrayList[String]), Set("key1", "key3"))
      }
    }

    "extract from a struct" in {
      val format = RecordFormat[SomeTest]
      val avro = format.to(SomeTest("abc", 12.5, Seq.empty, Map.empty))
      val struct = avroData.toConnectData(avro.getSchema, avro)
      KeysExtractor.fromStruct(struct.value().asInstanceOf[Struct], Set("name")) shouldBe
        Set("name" -> "abc")
    }

    "extract from a struct should throw an exception if a key is an array" in {
      val format = RecordFormat[SomeTest]
      val avro = format.to(SomeTest("abc", 12.5, Seq.empty, Map.empty))
      intercept[ConfigException] {
        val struct = avroData.toConnectData(avro.getSchema, avro)
        KeysExtractor.fromStruct(struct.value().asInstanceOf[Struct], Set("flags"))
      }
    }

    "extract from a struct should throw an exception if a key is a map" in {
      val format = RecordFormat[SomeTest]
      val avro = format.to(SomeTest("abc", 12.5, Seq.empty, Map.empty))
      intercept[ConfigException] {
        val struct = avroData.toConnectData(avro.getSchema, avro)
        KeysExtractor.fromStruct(struct.value().asInstanceOf[Struct], Set("map"))
      }
    }

    "extract from a struct should throw an exception if a key is a struct" in {
      val format = RecordFormat[WithNested]
      val avro = format.to(WithNested(1, SomeTest("abc", 12.5, Seq.empty, Map.empty)))
      intercept[ConfigException] {
        val struct = avroData.toConnectData(avro.getSchema, avro)
        KeysExtractor.fromStruct(struct.value().asInstanceOf[Struct], Set("nested"))
      }
    }
  }
}
