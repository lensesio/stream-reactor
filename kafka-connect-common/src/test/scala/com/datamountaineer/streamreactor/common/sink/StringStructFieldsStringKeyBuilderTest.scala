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

package com.datamountaineer.streamreactor.common.sink

import com.datamountaineer.streamreactor.common.rowkeys.StringStructFieldsStringKeyBuilder
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StringStructFieldsStringKeyBuilderTest extends AnyWordSpec with Matchers {
  "StructFieldsStringKeyBuilder" should {
    "raise an exception if the field is not present in the struct" in {
      intercept[IllegalArgumentException] {
        val schema = SchemaBuilder.struct().name("com.example.Person")
          .field("firstName", Schema.STRING_SCHEMA)
          .field("age", Schema.INT32_SCHEMA)
          .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

        val struct = new Struct(schema).put("firstName", "Alex").put("age", 30)

        val sinkRecord = new SinkRecord("sometopic", 1, null, null, schema, struct, 1)
        StringStructFieldsStringKeyBuilder(Seq("threshold")).build(sinkRecord)
      }
    }

    "create the row key based on one single field in the struct" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct = new Struct(schema).put("firstName", "Alex").put("age", 30)

      val sinkRecord = new SinkRecord("sometopic", 1, null, null, schema, struct, 1)
      StringStructFieldsStringKeyBuilder(Seq("firstName")).build(sinkRecord) shouldBe "Alex"
    }

    "create the row key based on one single field with doc in the struct" in {
      val firstNameSchema = SchemaBuilder.`type`(Schema.Type.STRING).doc("first name")
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", firstNameSchema)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct = new Struct(schema).put("firstName", "Alex").put("age", 30)

      val sinkRecord = new SinkRecord("sometopic", 1, null, null, schema, struct, 1)
      StringStructFieldsStringKeyBuilder(Seq("firstName")).build(sinkRecord) shouldBe "Alex"
    }

    "create the row key based on more thant one field in the struct" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct = new Struct(schema).put("firstName", "Alex").put("age", 30)

      val sinkRecord = new SinkRecord("sometopic", 1, null, null, schema, struct, 1)
      StringStructFieldsStringKeyBuilder(Seq("firstName", "age")).build(sinkRecord) shouldBe "Alex.30"
    }
  }
}
