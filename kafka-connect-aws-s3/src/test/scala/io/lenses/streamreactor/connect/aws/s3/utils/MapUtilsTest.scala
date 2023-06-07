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
package io.lenses.streamreactor.connect.aws.s3.utils

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MapUtilsTest extends AnyFunSuite with Matchers {
  test("merging and overwriting keys") {
    val context    = Map("a" -> "1", "b" -> "2")
    val properties = Map("a" -> "3", "c" -> "4")
    val result     = MapUtils.mergeProps(context, properties)
    result shouldBe Map("a" -> "1", "b" -> "2", "c" -> "4")
  }
  test("handle empty context map") {
    val map1   = Map.empty[String, String]
    val map2   = Map("a" -> "3", "c" -> "4")
    val result = MapUtils.mergeProps(map1, map2)
    result shouldBe Map("a" -> "3", "c" -> "4")
  }
  test("handle empty props map") {
    val map1   = Map("a" -> "1", "b" -> "2")
    val map2   = Map.empty[String, String]
    val result = MapUtils.mergeProps(map1, map2)
    result shouldBe Map("a" -> "1", "b" -> "2")
  }
  test("handle empty maps") {
    val map1   = Map.empty[String, String]
    val map2   = Map.empty[String, String]
    val result = MapUtils.mergeProps(map1, map2)
    result shouldBe Map.empty[String, String]
  }
}
