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
package com.landoop.streamreactor.connect.hive

import cats.data.NonEmptyList
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DefaultPartitionLocationTest extends AnyFunSuite with Matchers {
  test("show should generate path using the standard metastore pattern") {
    val p1 = (PartitionKey("country"), "usa")
    val p2 = (PartitionKey("city"), "philly")
    DefaultPartitionLocation.show(Partition(NonEmptyList.of(p1, p2), None)) shouldBe "country=usa/city=philly"
  }
}
