package com.datamountaineer.streamreactor.connect.mqtt.source.converters

import com.datamountaineer.streamreactor.connect.mqtt.source.MqttMsgKey
import org.apache.kafka.connect.data.Schema
import org.scalatest.{Matchers, WordSpec}

class BytesConverterTest extends WordSpec with Matchers {
  private val converter = new BytesConverter()
  private val topic = "topicA"

  "BytesConverter" should {
    "handle null payloads" in {
      val sourceRecord = converter.convert(topic, "somesource", 100, null)

      sourceRecord.keySchema() shouldBe MqttMsgKey.schema
      sourceRecord.key() shouldBe MqttMsgKey.getStruct("somesource", 100)
      sourceRecord.valueSchema() shouldBe Schema.BYTES_SCHEMA
      sourceRecord.value() shouldBe null
    }

    "handle non-null payloads" in {
      val expectedPayload: Array[Byte] = Array(245, 2, 10, 200, 22, 0, 0, 11).map(_.toByte)
      val sourceRecord = converter.convert(topic, "somesource", 1001, expectedPayload)

      sourceRecord.keySchema() shouldBe MqttMsgKey.schema
      sourceRecord.key() shouldBe MqttMsgKey.getStruct("somesource", 1001)
      sourceRecord.valueSchema() shouldBe Schema.BYTES_SCHEMA
      sourceRecord.value() shouldBe expectedPayload
    }
  }
}
