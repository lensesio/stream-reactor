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

import com.datamountaineer.streamreactor.connect.bloomberg.avro.AvroSchemaGenerator._
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.data.Schema
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class SourceRecordConverterFnTest extends WordSpec with Matchers {
  "SourceRecordConverterFn" should {
    "handle null kafka topic parameter" in {
      intercept[IllegalArgumentException] {
        val data = BloombergData(Map.empty[String, Any].asJava)
        data.toJsonSourceRecord(null)
      }
    }

    "handle empty kafka topic" in {
      intercept[IllegalArgumentException] {
        val data = BloombergData(Map.empty[String, Any].asJava)
        data.toJsonSourceRecord(" ")
      }
    }

    "convert a BloombergSubscriptionData to SourceRecord with json" in {
      val values = Map(
        "field1" -> 1,
        "field2" -> "some text",
        "field3" -> Seq(1.3, 1.6, -2.0).asJava,
        "field4" -> Map(
          "m2field1" -> 200,
          "m2field2" -> "abc").asJava).asJava

      val topic = "destination"
      val data = BloombergData(values)

      val record = data.toJsonSourceRecord(topic)

      record.keySchema() shouldBe null
      record.key() shouldBe null //for now we don't support it
      record.topic() shouldBe topic

      record.valueSchema() shouldBe Schema.STRING_SCHEMA
      record.value() shouldBe "{\"field1\":1,\"field2\":\"some text\",\"field3\":[1.3,1.6,-2.0],\"field4\":{\"m2field1\":200,\"m2field2\":\"abc\"}}"
    }

    "convert a BloombergSubscriptionData to SourceRecord with avro" in {

      val map = new util.LinkedHashMap[String, Any]
      val ticker = "MSFT Equity"
      val firstName = "Isabela"
      val lastName = "Smith"
      val age = 25
      map.put(BloombergData.SubscriptionFieldKey, ticker)
      map.put("firstName", firstName)
      map.put("lastName", lastName)
      map.put("age", age)

      val streetAddress = "21 2nd Street"
      val city = "New York"
      val postalCode = "10021"
      val mapAddress = new util.LinkedHashMap[String, Any]()
      mapAddress.put("streetAddress", streetAddress)
      mapAddress.put("city", city)
      mapAddress.put("state", "NY")
      mapAddress.put("postalCode", postalCode)

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

      val data = BloombergData(map)

      val kafkaTopic = "some_kafka_topic"
      val sourceRecord = data.toAvroSourceRecord(kafkaTopic)

      sourceRecord.keySchema() shouldBe null
      sourceRecord.key() shouldBe null //for now we don't support it
      sourceRecord.topic() shouldBe kafkaTopic

      sourceRecord.valueSchema() shouldBe Schema.BYTES_SCHEMA

      val avro = sourceRecord.value().asInstanceOf[Array[Byte]]
      val genericRecord = AvroDeserializer(avro, data.getSchema)

      genericRecord.get(BloombergData.SubscriptionFieldKey) shouldBe ticker
      genericRecord.get("firstName") shouldBe firstName
      genericRecord.get("lastName") shouldBe lastName
      genericRecord.get("age") shouldBe age

      val addressRecord = genericRecord.get("address").asInstanceOf[GenericRecord]
      addressRecord should not be null
    }
  }
}
