/*
 *  Copyright 2017 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.schemas

import com.datamountaineer.streamreactor.common.schemas.StructFieldsExtractor
import org.apache.kafka.connect.data.{Date, Schema, SchemaBuilder, Struct}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StructFieldExtractorTest extends AnyWordSpec with Matchers {
  "StructFieldExtractor" should {
    "return all the fields and their bytes value" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct = new Struct(schema)
        .put("firstName", "Alex")
        .put("lastName", "Smith")
        .put("age", 30)

      val map = new StructFieldsExtractor(true, Map.empty).get(struct).toMap

      map.get("firstName").get shouldBe "Alex"
      map.get("lastName").get shouldBe "Smith"
      map.get("age").get shouldBe 30
    }

    "return all fields and apply the mapping" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct = new Struct(schema)
        .put("firstName", "Alex")
        .put("lastName", "Smith")
        .put("age", 30)

      val map = new StructFieldsExtractor(true, Map("lastName" -> "Name", "age" -> "a")).get(struct).toMap

      map.get("firstName").get shouldBe "Alex"
      map.get("Name").get shouldBe "Smith"
      map.get("a").get shouldBe 30

    }

    "return only the specified fields" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct = new Struct(schema)
        .put("firstName", "Alex")
        .put("lastName", "Smith")
        .put("age", 30)

      val map = new StructFieldsExtractor(false, Map("lastName" -> "Name", "age" -> "age")).get(struct).toMap

      map.get("Name").get shouldBe "Smith"
      map.get("age").get shouldBe 30

      map.size shouldBe 2
    }
  }

  "handle Date fieldds" in {
    val dateSchema = Date.builder().build()
    val schema = SchemaBuilder.struct().name("com.example.Person")
      .field("firstName", Schema.STRING_SCHEMA)
      .field("lastName", Schema.STRING_SCHEMA)
      .field("age", Schema.INT32_SCHEMA)
      .field("date", dateSchema).build()

    val date =  java.sql.Date.valueOf("2017-04-25")
    val struct = new Struct(schema)
      .put("firstName", "Alex")
      .put("lastName", "Smith")
      .put("age", 30)
      .put("date", date)

    val map1 = new StructFieldsExtractor(false, Map("date" -> "date")).get(struct).toMap
    map1.get("date").get shouldBe date
    map1.size shouldBe 1

    val d = Date.toLogical(dateSchema, 10000)
    struct.put("date", d)

    val map2 = new StructFieldsExtractor(false, Map("date" -> "date")).get(struct).toMap
    map2.get("date").get shouldBe d
    map2.size shouldBe 1

  }

}
