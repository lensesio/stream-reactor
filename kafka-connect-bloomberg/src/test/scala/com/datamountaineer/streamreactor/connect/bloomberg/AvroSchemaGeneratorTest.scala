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

package com.datamountaineer.streamreactor.connect.bloomberg

import java.util

import com.datamountaineer.streamreactor.connect.bloomberg.avro.AvroSchemaGenerator
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.codehaus.jackson.JsonNode
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class AvroSchemaGeneratorTest extends WordSpec with Matchers {
  val namespace = "io.confluent.connect.avro"
  val schemaGenerator = new AvroSchemaGenerator(namespace)

  def setString(schema: Schema): Schema = {
    GenericData.setStringType(schema, GenericData.StringType.String)
    schema
  }

  "AvroSchema" should {
    "handle boolean input" in {
      schemaGenerator.create("ConnectDefault", true) shouldBe Schema.create(Schema.Type.BOOLEAN)
      schemaGenerator.create("ConnectDefault", false) shouldBe Schema.create(Schema.Type.BOOLEAN)
    }
    "handle char input" in {
      schemaGenerator.create("ConnectDefault", 'a') shouldBe setString(Schema.create(Schema.Type.STRING))
    }
    "handle string input" in {
      schemaGenerator.create("ConnectDefault", "cosmic gate") shouldBe setString(Schema.create(Schema.Type.STRING))
    }
    "handle long input" in {
      schemaGenerator.create("ConnectDefault", 1L) shouldBe Schema.create(Schema.Type.LONG)
    }
    "handle float input" in {
      schemaGenerator.create("ConnectDefault", 34.5f) shouldBe Schema.create(Schema.Type.FLOAT)
    }
    "handle double input" in {
      schemaGenerator.create("ConnectDefault", -324.23d) shouldBe Schema.create(Schema.Type.DOUBLE)
    }

    "handle List[int] input" in {
      schemaGenerator.create("ConnectDefault", Seq(1, 2, 3).asJava) shouldBe Schema.createArray(Schema.create(Schema.Type.INT))
    }

    "handle LinkedHashMap[String,Any] input" in {
      val map = new java.util.LinkedHashMap[String, Any]
      map.put("k1", 1)
      map.put("k2", "minime")

      val expectedSchema = Schema.createRecord("ConnectDefault", null, namespace, false)
      val default: JsonNode = null
      val fields = Seq(
        new Schema.Field("k1", AvroSchemaGenerator.optionalSchema(Schema.Type.INT), null, default),
        new Schema.Field("k2", AvroSchemaGenerator.optionalSchema(Schema.Type.STRING), null, default)
      ).asJava
      expectedSchema.setFields(fields)

      val actualSchema = schemaGenerator.create("ConnectDefault", map)
      actualSchema shouldBe expectedSchema
    }

    "raise an error if the input is not long, float,char, string,LinkedHashMap[String, Any],List[Any]" in {
      intercept[RuntimeException] {
        schemaGenerator.create("ConnectDefault", BigDecimal(131))
      }
      intercept[RuntimeException] {
        schemaGenerator.create("ConnectDefault", Map("s" -> 11).asJava)
      }
    }


    "create the appropriate schema for the given linkedhashmap entry" in {
      val map = new util.LinkedHashMap[String, Any]()
      map.put("firstName", "John")
      map.put("lastName", "Smith")
      map.put("age", 25)

      val mapAddress = new util.LinkedHashMap[String, Any]()
      mapAddress.put("streetAddress", "21 2nd Street")
      mapAddress.put("city", "New York")
      mapAddress.put("state", "NY")
      mapAddress.put("postalCode", "10021")

      map.put("address", mapAddress)

      val phoneMap = new util.LinkedHashMap[String, Any]()
      phoneMap.put("type", "home")
      phoneMap.put("number", "212 555-1234")


      val faxMap = new util.LinkedHashMap[String, Any]()
      faxMap.put("type", "fax")
      faxMap.put("number", "646 555-4567")

      map.put("phoneNumber", Seq(phoneMap, faxMap).asJava)

      val genderMap = new java.util.LinkedHashMap[String, Any]()
      genderMap.put("type", "male")
      map.put("gender", genderMap)

      val actualSchema = schemaGenerator.create("ConnectDefault", map)

      val expectedSchema = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream(s"/person.avsc"))

      actualSchema.toString(true) shouldBe expectedSchema.toString(true)
    }
  }
}

object AvroSchemaGeneratorTest {
  def deserializeAvroRecord(data: Array[Byte], schema: Schema): GenericRecord = {
    val reader = new GenericDatumReader[GenericRecord](schema)
    val decoder = DecoderFactory.get().binaryDecoder(data, null)
    reader.read(null, decoder)
  }
}
