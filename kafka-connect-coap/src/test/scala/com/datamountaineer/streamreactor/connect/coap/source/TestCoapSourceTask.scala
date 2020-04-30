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

package com.datamountaineer.streamreactor.connect.coap.source

import java.util.logging.Level

import com.datamountaineer.streamreactor.connect.coap.{Server, TestBase}
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceTaskContext
import org.eclipse.californium.core.{CaliforniumLogger, CoapClient}
import org.eclipse.californium.scandium.ScandiumLogger
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 28/12/2016. 
  * stream-reactor
  */
class TestCoapSourceTask extends AnyWordSpec with BeforeAndAfter with TestBase with MockitoSugar {
  val server = new Server(SOURCE_PORT_SECURE, SOURCE_PORT_INSECURE, KEY_PORT_INSECURE)

  before {
    server.start()
    Thread.sleep(5000)
  }

  after {
    server.stop()
  }

  CaliforniumLogger.initialize()
  CaliforniumLogger.setLevel(Level.INFO)
  ScandiumLogger.initialize()
  ScandiumLogger.setLevel(Level.INFO)

//  "should create a secure task and read a message" in {
//    val props = getPropsSecure
//    val producerConfig = CoapSourceConfig(getTestSourceProps)
//    val producerSettings = CoapSettings(producerConfig)
//
//    val task = new CoapSourceTask
//    task.start(props)
//
//    //get secure client to put messages in
//    val dtlsConnector = new DTLSConnector(DTLSConnectionFn(producerSettings.head))
//    val client = new CoapClient(new URI(s"$SOURCE_URI_SECURE/$RESOURCE_SECURE"))
//    client.setEndpoint(new CoapEndpoint(dtlsConnector, NetworkConfig.getStandard()))
//    client.post("Message1", 0)
//    Thread.sleep(5000)
//
////    //ask for records
//    val records = task.poll()
//    records.size() shouldBe 1
//    val record = records.head
//    val struct = record.value().asInstanceOf[Struct]
//    struct.getString("payload") shouldBe "Message1"
//    struct.getString("type") shouldBe "ACK"
//    task.stop
//  }

  "should create a task and receive messages" in {
      val props = getPropsInsecure
      val task = new CoapSourceTask()
      val context = mock[SourceTaskContext]
      when(context.configs()).thenReturn(props)
      task.initialize(context)
      task.start(props)
      Thread.sleep(1000)
      val records = task.poll()
      records.size() shouldBe 0

      val client = new CoapClient(s"$SOURCE_URI_INSECURE/$RESOURCE_INSECURE")
      client.post("Message1", 0)
      Thread.sleep(3000)
      val records2 = task.poll()
      records2.size() shouldBe 1
      val record2 = records2.asScala.head
      val struct2 = record2.value().asInstanceOf[Struct]
      struct2.getString("payload") shouldBe "Message1"
      struct2.getString("type") shouldBe "ACK"

      client.post("Message2", 0)
      Thread.sleep(3000)
      val records3 = task.poll()
      records3.size() shouldBe 1
      val record3 = records3.asScala.head
      val struct3 = record3.value().asInstanceOf[Struct]
      struct3.getString("payload") shouldBe "Message2"
      struct3.getString("type") shouldBe "ACK"
      task.stop()
  }
}
