package com.datamountaineer.streamreactor.connect.bloomberg

import org.apache.kafka.connect.data.Schema
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class SourceRecordConverterFnTest extends WordSpec with Matchers {
  "SourceRecordConverterFn" should {
    "handle null kafka topic parameter" in {
      intercept[IllegalArgumentException] {
        val data = BloombergData("ticker", Map.empty[String, Any].asJava)
        SourceRecordConverterFn(data, null)
      }
    }

    "handle empty kafka topic" in {
      intercept[IllegalArgumentException] {
        val data = BloombergData("ticker", Map.empty[String, Any].asJava)
        SourceRecordConverterFn(data, " ")
      }
    }

    "convert a BloombergSubscriptionData to SourceRecord" in {
      val values = Map(
        "field1" -> 1,
        "field2" -> "some text",
        "field3" -> Seq(1.3, 1.6, -2.0).asJava,
        "field4" -> Map(
          "m2field1" -> 200,
          "m2field2" -> "abc").asJava).asJava

      val ticker = "ticker1"
      val topic = "destination"
      val data = BloombergData(ticker, values)

      val record = SourceRecordConverterFn(data, topic)

      record.keySchema() shouldBe null
      record.key() shouldBe null //for now we don't support it
      record.topic() shouldBe topic

      record.valueSchema() shouldBe Schema.STRING_SCHEMA
      record.value() shouldBe "{\"field1\":1,\"field2\":\"some text\",\"field3\":[1.3,1.6,-2.0],\"field4\":{\"m2field1\":200,\"m2field2\":\"abc\"}}"
    }
  }
}
