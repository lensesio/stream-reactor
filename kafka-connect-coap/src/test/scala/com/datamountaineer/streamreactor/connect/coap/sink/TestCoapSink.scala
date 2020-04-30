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

package com.datamountaineer.streamreactor.connect.coap.sink

import com.datamountaineer.streamreactor.connect.coap.{Server, TestBase}
import com.datamountaineer.streamreactor.connect.converters.source.SinkRecordToJson
import org.apache.kafka.connect.sink.SinkTaskContext
import org.eclipse.californium.core.CoapClient
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 29/12/2016.
  * stream-reactor
  */
class TestCoapSink extends AnyWordSpec  with BeforeAndAfter with TestBase with MockitoSugar {
  val server = new Server(SINK_PORT_SECURE, SINK_PORT_INSECURE, KEY_PORT_INSECURE)

  before { server.start() }
  after { server.stop() }

  "should start a CoapSink and Write an insecure Coap server" in {
    val props = getPropsInsecureSink
    val task = new CoapSinkTask


    val context = mock[SinkTaskContext]
    when(context.configs()).thenReturn(props)
    task.initialize(context)
    task.start(props)
    val records = getTestRecords(10)

    val json = records.map(r => SinkRecordToJson(r, Map.empty, Map.empty))
    task.put(records.asJava)
    val client = new CoapClient(s"$SINK_URI_INSECURE/$RESOURCE_INSECURE")

    val response1 = client.get()
    json.contains(response1.getResponseText) shouldBe true
    val response2 = client.get()
    json.contains(response2.getResponseText) shouldBe true
    val response3 = client.get()
    json.contains(response3.getResponseText) shouldBe true
    val response4 = client.get()
    json.contains(response4.getResponseText) shouldBe true
    val response5 = client.get()
    json.contains(response5.getResponseText) shouldBe true
    val response6 = client.get()
    json.contains(response6.getResponseText) shouldBe true
    val response7 = client.get()
    json.contains(response7.getResponseText) shouldBe true
    val response8 = client.get()
    json.contains(response8.getResponseText) shouldBe true
    val response9 = client.get()
    json.contains(response9.getResponseText) shouldBe true
    val response10 = client.get()
    json.contains(response10.getResponseText) shouldBe true

    task.stop()
  }


//  "should start a CoapSink and Write an secure Coap server" in {
//    val props = getPropsSinkSecure
//    val task = new CoapSinkTask
//    task.start(props)
//    val records = getTestRecords(10)
//
//    val json = records.map(r => SinkRecordToJson(r, Map.empty, Map.empty))
//    task.put(records.asJava)
//
//    val producerConfig = CoapSinkConfig(getTestSink)
//    val producerSettings = CoapSettings(producerConfig)
//    val dtlsConnector = new DTLSConnector(DTLSConnectionFn(producerSettings.head))
//    val client = new CoapClient(s"$SINK_URI_SECURE/$RESOURCE_SECURE")
//    client.setEndpoint(new CoapEndpoint(dtlsConnector, NetworkConfig.getStandard()))
//
//    val response1 = client.get()
//    json.contains(response1.getResponseText) shouldBe true
//    val response2 = client.get()
//    json.contains(response2.getResponseText) shouldBe true
//    val response3 = client.get()
//    json.contains(response3.getResponseText) shouldBe true
//    val response4 = client.get()
//    json.contains(response4.getResponseText) shouldBe true
//    val response5 = client.get()
//    json.contains(response5.getResponseText) shouldBe true
//    val response6 = client.get()
//    json.contains(response6.getResponseText) shouldBe true
//    val response7 = client.get()
//    json.contains(response7.getResponseText) shouldBe true
//    val response8 = client.get()
//    json.contains(response8.getResponseText) shouldBe true
//    val response9 = client.get()
//    json.contains(response9.getResponseText) shouldBe true
//    val response10 = client.get()
//    json.contains(response10.getResponseText) shouldBe true
//    val response11 = client.get()
//    response11 shouldBe null
//
//    task.stop()
//  }

}
