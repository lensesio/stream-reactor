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

package com.datamountaineer.streamreactor.connect.hbase.avro

import java.nio.file.Paths

import org.apache.avro.Schema
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.{Matchers, WordSpec}

class AvroRecordFieldExtractorMapFnTest extends WordSpec with Matchers {

  val schema: Schema = new Schema.Parser().parse(Paths.get(getClass.getResource("/person.avsc").toURI).toFile)

  "AvroRecordFieldExtractorMapFn" should {
    "raise an exception if the given field does not exist in the schema" in {
      intercept[IllegalArgumentException] {
        AvroRecordFieldExtractorMapFn(schema, Seq("wrongField"))
      }
    }

    "raise an exception if the given field is not a primitive" in {
      intercept[IllegalArgumentException] {
        AvroRecordFieldExtractorMapFn(schema, Seq("address"))
      }
    }

    "create the mappings for all the given fields" in {
      val mappings = AvroRecordFieldExtractorMapFn(schema, Seq("firstName", "age"))

      val fnFirstName = mappings("firstName")
      val firstName = "Beaky"
      fnFirstName(firstName) shouldBe Bytes.toBytes(firstName)

      val fnAge = mappings("age")
      val age = 31
      fnAge(age) shouldBe Bytes.toBytes(age)
      intercept[ClassCastException] {
        fnAge(12.4)
      }
    }
  }
}
