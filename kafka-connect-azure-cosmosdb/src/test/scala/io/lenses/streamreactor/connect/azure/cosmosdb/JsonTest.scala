/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.azure.cosmosdb

import com.fasterxml.jackson.annotation.JsonProperty
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.json4s.MappingException

case class TestData(
  @JsonProperty("name") name:   String,
  @JsonProperty("value") value: Int,
)

class JsonTest extends AnyFunSuite with Matchers {

  test("convert valid JSON string to case class instance") {
    val json   = """{"name":"example","value":42}"""
    val result = Json.fromJson[TestData](json)
    result shouldEqual TestData("example", 42)
  }

  test("convert case class instance to JSON string") {
    val data   = TestData("example", 42)
    val result = Json.toJson(data)
    result shouldEqual """{"name":"example","value":42}"""
  }

  test("throw exception when converting invalid JSON string to case class") {
    val invalidJson = """{"invalid":"data"}"""
    an[MappingException] should be thrownBy Json.fromJson[TestData](invalidJson)
  }

  test("handle empty JSON string gracefully") {
    val emptyJson = ""
    an[Exception] should be thrownBy Json.fromJson[TestData](emptyJson)
  }

  test("handle null JSON string gracefully") {
    val nullJson: String = null
    an[Exception] should be thrownBy Json.fromJson[TestData](nullJson)
  }
}
