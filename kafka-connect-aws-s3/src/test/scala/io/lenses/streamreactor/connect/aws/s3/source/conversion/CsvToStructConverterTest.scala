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

package io.lenses.streamreactor.connect.aws.s3.source.conversion

import org.apache.kafka.connect.data.{Field, Schema}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CsvToStructConverterTest extends AnyFlatSpec with Matchers {

  "convert" should "convert records to a struct" in {
    val struct = CsvToStructConverter.convertToStruct(List("name", "title", "salary"), List("sam", "mr", "100.43"))
    struct.schema().fields() should contain allElementsOf List(
      new Field("name", 0, Schema.OPTIONAL_STRING_SCHEMA),
      new Field("title", 1, Schema.OPTIONAL_STRING_SCHEMA),
      new Field("salary", 2, Schema.OPTIONAL_STRING_SCHEMA)
    )
    struct.get("name") should be("sam")
    struct.get("title") should be("mr")
    struct.get("salary") should be("100.43")
  }
}

