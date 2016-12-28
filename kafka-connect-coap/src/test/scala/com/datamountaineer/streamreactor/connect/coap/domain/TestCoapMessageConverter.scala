package com.datamountaineer.streamreactor.connect.coap.domain

import com.datamountaineer.streamreactor.connect.coap.TestBase
import org.apache.kafka.connect.data.Struct
import org.scalatest.WordSpec

/**
  * Created by andrew@datamountaineer.com on 28/12/2016. 
  * stream-reactor
  */
class TestCoapMessageConverter extends WordSpec with TestBase {
  "should convert a CoapResponse to a Struct " in {
    val response = getCoapResponse()
    val converter = new CoapMessageConverter
    val record = converter.convert(TOPIC, response)
    val struct = record.value().asInstanceOf[Struct]
    struct.getString("payload") shouldBe response.getPayloadString
    struct.getInt32("raw_code") shouldBe response.getRawCode
    struct.getBoolean("is_last") shouldBe response.isLast
    struct.getInt32("content_format") shouldBe response.getOptions.getContentFormat
  }
}
