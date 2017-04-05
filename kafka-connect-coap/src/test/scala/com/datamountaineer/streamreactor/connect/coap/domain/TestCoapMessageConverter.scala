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
    val response = getCoapResponse
    val converter = new CoapMessageConverter
    val record = converter.convert(RESOURCE_INSECURE ,TOPIC, response)
    val struct = record.value().asInstanceOf[Struct]
    struct.getString("payload") shouldBe response.getPayloadString
    struct.getInt32("raw_code") shouldBe response.getRawCode
    struct.getBoolean("is_last") shouldBe response.isLast
    struct.getInt32("content_format") shouldBe response.getOptions.getContentFormat
  }
}
