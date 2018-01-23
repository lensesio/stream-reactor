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

package com.datamountaineer.streamreactor.connect.elastic6.indexname

import org.scalatest.prop.{GeneratorDrivenPropertyChecks, TableDrivenPropertyChecks}
import org.scalatest.{FlatSpec, Matchers}

class TestCustomIndexName extends FlatSpec with TableDrivenPropertyChecks with GeneratorDrivenPropertyChecks with Matchers {

  val ValidIndexNames = Table(
    ("Valid index name", "Expectations"),
    ("", Vector()),
    ("abc", Vector(TextFragment("abc"))),
    ("abc{YYYY-MM-dd}", Vector(TextFragment("abc"), DateTimeFragment("YYYY-MM-dd"))),
    ("{YYYY-MM-dd}abc", Vector(DateTimeFragment("YYYY-MM-dd"), TextFragment("abc"))),
    ("{YYYY-MM-dd}abc{HH-MM-ss}", Vector(DateTimeFragment("YYYY-MM-dd"), TextFragment("abc"), DateTimeFragment("HH-MM-ss"))),
    ("{YYYY-MM-dd}{HH-MM-ss}", Vector(DateTimeFragment("YYYY-MM-dd"), DateTimeFragment("HH-MM-ss"))),
    ("abc{}", Vector(TextFragment("abc"))),
    ("{}abc", Vector(TextFragment("abc")))
  )

  val InvalidIndexNames = Table(
    ("Invalid index name"),
    ("}abc"),
    ("abc}"),
    ("abc}def")
  )

  "Custom index name" should "parse a valid String with date time formatting options" in {
    forAll (ValidIndexNames) { case (validIndexName, expectations) =>
      CustomIndexName.parseIndexName(validIndexName) shouldBe CustomIndexName(expectations)
    }
  }

  it should "throw an exception when using invalid index name" in {
    forAll (InvalidIndexNames) { case (invalidIndexName) =>
      intercept[InvalidCustomIndexNameException] {
        CustomIndexName.parseIndexName(invalidIndexName)
      }
    }
  }

  it should "return a valid String from a list of fragments" in new ClockFixture {
    CustomIndexName(
      Vector(DateTimeFragment("YYYY-MM-dd", TestClock),
        TextFragment("ABC"),
        DateTimeFragment("HH:mm:ss", TestClock))
    ).toString shouldBe "2016-10-02ABC14:00:00"
  }
}
