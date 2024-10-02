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
package io.lenses.streamreactor.connect.http.sink.tpl

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JsonTidyTest extends AnyFlatSpec with Matchers {

  "removeTrailingCommas" should "handle empty JSON string" in {
    val jsonString = ""
    val result     = JsonTidy.cleanUp(jsonString)
    result shouldEqual jsonString
  }

  it should "handle JSON object without trailing commas" in {
    val jsonString = """{"key1":"value1","key2":"value2"}"""
    val result     = JsonTidy.cleanUp(jsonString)
    result shouldEqual jsonString
  }

  it should "remove trailing commas in JSON object" in {
    val jsonString = """{"key1":"value1","key2":"value2",}"""
    val expected   = """{"key1":"value1","key2":"value2"}"""
    val result     = JsonTidy.cleanUp(jsonString)
    result shouldEqual expected
  }

  it should "handle JSON array without trailing commas" in {
    val jsonString = """["value1","value2"]"""
    val result     = JsonTidy.cleanUp(jsonString)
    result shouldEqual jsonString
  }

  it should "remove trailing commas in JSON array" in {
    val jsonString = """["value1","value2",]"""
    val expected   = """["value1","value2"]"""
    val result     = JsonTidy.cleanUp(jsonString)
    result shouldEqual expected
  }

  it should "handle nested JSON structures" in {
    val jsonString = """{"key1":["value1","value2",],"key2":{"subkey1":"subvalue1",},}"""
    val expected   = """{"key1":["value1","value2"],"key2":{"subkey1":"subvalue1"}}"""
    val result     = JsonTidy.cleanUp(jsonString)
    result shouldEqual expected
  }
}
