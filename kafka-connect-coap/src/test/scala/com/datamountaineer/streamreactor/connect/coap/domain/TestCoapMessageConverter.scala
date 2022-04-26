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
import org.eclipse.californium.core.coap.CoAP.{ResponseCode, Type}
import org.eclipse.californium.core.coap.{MediaTypeRegistry, OptionSet, Response}
import org.eclipse.californium.elements.AddressEndpointContext
import org.scalatest.wordspec.AnyWordSpec

import java.net.InetAddress

/**
  * Created by andrew@datamountaineer.com on 28/12/2016. 
  * stream-reactor
  */
class TestCoapMessageConverter extends AnyWordSpec with TestBase {
  "should convert a CoapResponse to a Struct " in {
    val response = getCoapResponse
    val converter = new CoapMessageConverter
    val record = converter.convert(RESOURCE_INSECURE ,TOPIC, response)
    val struct = record.value().asInstanceOf[Struct]
    struct.getString("payload") shouldBe response.getPayloadString
    struct.getInt32("raw_code") shouldBe response.getRawCode
    struct.getInt32("content_format") shouldBe response.getOptions.getContentFormat
  }

  def getCoapResponse: Response = {
    val response = new Response(ResponseCode.CREATED)

    response.setApplicationRttNanos(1L)
    response.setAcknowledged(false)
    response.setCanceled(false)
    response.setConfirmable(false)

    response.setDestinationContext(new AddressEndpointContext(InetAddress.getLocalHost, SOURCE_PORT_INSECURE))
    response.setDuplicate(false)
    response.setMID(1)
    response.setPayload("TEST PAYLOAD")
    response.setRejected(false)

    response.setTimedOut(false)
    response.setNanoTimestamp(0)
    response.setToken("token".getBytes)
    response.setType(Type.NON)

    val options = new OptionSet()
    options.setAccept(1)
    options.setBlock1("b1".getBytes)
    options.setBlock2("b2".getBytes)
    options.setContentFormat(MediaTypeRegistry.TEXT_PLAIN)
    options.setLocationPath("loc_path")
    options.setLocationQuery("loc_query")
    options.setMaxAge(100)
    options.setObserve(2)
    options.setProxyUri("proxy_uri")
    options.setSize1(1)
    options.setSize2(2)
    options.setUriHost("uri_host")
    options.setUriPort(99)
    options.setUriPath("uri_path")
    options.setUriPort(99)
    options.setUriQuery("uri_query")
    options.addETag("TestETag".getBytes)

    response.setOptions(options)
    response
  }

}
