/*
 * *
 *   * Copyright 2016 Datamountaineer.
 *   *
 *   * Licensed under the Apache License, Version 2.0 (the "License");
 *   * you may not use this file except in compliance with the License.
 *   * You may obtain a copy of the License at
 *   *
 *   * http://www.apache.org/licenses/LICENSE-2.0
 *   *
 *   * Unless required by applicable law or agreed to in writing, software
 *   * distributed under the License is distributed on an "AS IS" BASIS,
 *   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   * See the License for the specific language governing permissions and
 *   * limitations under the License.
 *   *
 */

package com.datamountaineer.streamreactor.connect.coap.source

import java.net.URI

import akka.actor.ActorSystem
import com.datamountaineer.streamreactor.connect.coap.{Server, TestBase}
import com.datamountaineer.streamreactor.connect.coap.configs.{CoapConfig, CoapSettings}
import org.apache.kafka.connect.data.Struct
import org.eclipse.californium.core.{CaliforniumLogger, CoapClient}
import akka.pattern.ask
import akka.util.Timeout
import java.util.logging.Level

import com.datamountaineer.streamreactor.connect.coap.connection.DTLSConnectionFn
import org.eclipse.californium.core.network.CoapEndpoint
import org.eclipse.californium.core.network.config.NetworkConfig
import org.eclipse.californium.scandium.{DTLSConnector, ScandiumLogger}

import scala.concurrent.duration._
import org.scalatest.{BeforeAndAfter, WordSpec}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  * Created by andrew@datamountaineer.com on 28/12/2016. 
  * stream-reactor
  */
class TestCoapSourceTask extends WordSpec with BeforeAndAfter with TestBase {
  val server = new Server(SOURCE_PORT_SECURE, SOURCE_PORT_INSECURE)

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

  "should create a secure reader and read a message" in {
    implicit val system = ActorSystem()
    implicit val timeout = Timeout(60 seconds)
    val props = getPropsSecure
    val config = CoapConfig(props)
    val producerConfig = CoapConfig(getTestSourceProps)
    val settings = CoapSettings(config, sink = false)
    val producerSettings = CoapSettings(producerConfig, sink = false)
    val actorProps = CoapReader(settings)
    val reader = system.actorOf(actorProps.head._2, actorProps.head._1)
    //start the reader
    reader ? StartChangeFeed

    //get secure client to put messages in
    val dtlsConnector = new DTLSConnector(DTLSConnectionFn(producerSettings.head))
    val client = new CoapClient(new URI(s"$SOURCE_URI_SECURE/$RESOURCE_SECURE"))
    client.setEndpoint(new CoapEndpoint(dtlsConnector, NetworkConfig.getStandard()))
    client.post("Message1", 0)
    Thread.sleep(5000)

    //ask for records
    val records = ActorHelper.askForRecords(reader)
    records.size() shouldBe 1
    val record = records.head
    val struct = record.value().asInstanceOf[Struct]
    struct.getString("payload") shouldBe "Message1"
    struct.getString("type") shouldBe "ACK"
    reader ? StopChangeFeed
  }

  "should create a task and receive messages" in {
      implicit val system = ActorSystem()
      implicit val timeout = Timeout(60 seconds)
      val props = getPropsInsecure
      val task = new CoapSourceTask()
      task.start(props)
      Thread.sleep(1000)
      val records = task.poll()
      records.size() shouldBe 0

      val client = new CoapClient(s"$SOURCE_URI_INSECURE/$RESOURCE_INSECURE")
      client.post("Message1", 0)
      Thread.sleep(3000)
      val records2 = task.poll()
      records2.size() shouldBe 1
      val record2 = records2.head
      val struct2 = record2.value().asInstanceOf[Struct]
      struct2.getString("payload") shouldBe "Message1"
      struct2.getString("type") shouldBe "ACK"

      client.post("Message2", 0)
      Thread.sleep(3000)
      val records3 = task.poll()
      records3.size() shouldBe 1
      val record3 = records3.head
      val struct3 = record3.value().asInstanceOf[Struct]
      struct3.getString("payload") shouldBe "Message2"
      struct3.getString("type") shouldBe "ACK"
      task.stop()
  }
}
