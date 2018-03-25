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

package com.datamountaineer.streamreactor.connect.blockchain.source

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.ws.{Message, TextMessage, UpgradeToWebSocket}
import akka.http.scaladsl.settings.ServerSettings
import akka.http.scaladsl.settings.ServerSettings.Timeouts
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.testkit.TestPublisher
import com.datamountaineer.streamreactor.connect.blockchain.config.BlockchainSettings
import com.datamountaineer.streamreactor.connect.blockchain.data.{BlockchainMessage, Transaction}
import com.datamountaineer.streamreactor.connect.blockchain.json.JacksonJson
import org.apache.kafka.connect.data.Struct
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.duration._

class BlockchainManagerTest extends WordSpec with Matchers {
  "BlockchainManager" should {
    "fail if it can not connect to the websocket" in {
      val manager = new BlockchainManager(BlockchainSettings("ws://localhost:12456", "blockchain", Set.empty))
      intercept[Throwable] {
        manager.start()
      }
    }

    "not close the connection if nothing is sent over the connection" in {
      implicit val system = ActorSystem()
      implicit val materializer = ActorMaterializer()

      val source = TestPublisher.probe[Message]()
      val serverSettings = ServerSettings(system)
        .withTimeouts(new Timeouts {
          override def idleTimeout: Duration = 2.seconds

          override def bindTimeout: FiniteDuration = 1.seconds

          override def requestTimeout: Duration = 1.seconds
        })
      val bindingFuture = Http().bindAndHandleSync({
        case HttpRequest(_, _, headers, _, _) =>
          val upgrade = headers.collectFirst { case u: UpgradeToWebSocket => u }.get
          upgrade.handleMessages(Flow.fromSinkAndSource(Sink.ignore, Source.fromPublisher(source)), None)
      }, interface = "localhost", port = 0)

      val binding = Await.result(bindingFuture, 2.seconds)
      val myPort = binding.localAddress.getPort

      val settings = BlockchainSettings(s"ws://localhost:$myPort",
        "blockchain" + System.currentTimeMillis(),
        Set.empty,
        keepAlive = 1.seconds)

      val manager = new BlockchainManager(settings)

      manager.start()

      Thread.sleep(3000)
      val json = scala.io.Source.fromFile(getClass.getResource("/transactions/transaction.json").toURI.getPath).mkString
      source
        .sendNext(TextMessage.Strict(json))
        .sendComplete()

      Thread.sleep(2000)

      val records = manager.get()
      Await.ready(binding.unbind(), 3.seconds)

      records.size() shouldBe 1
      records.get(0).kafkaPartition() shouldBe 0
      records.get(0).topic() shouldBe settings.kafkaTopic
      records.get(0).valueSchema() shouldBe Transaction.ConnectSchema
      val struct = records.get(0).value().asInstanceOf[Struct]
      struct.getInt64("time") shouldBe 1472769831

      materializer.shutdown()
      system.terminate()
    }
  }

  "provide all the transaction sent" in {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    val source = TestPublisher.probe[Message]()

    ServerSettings(system).withTimeouts(new Timeouts {
      override def idleTimeout: Duration = 2.seconds

      override def bindTimeout: FiniteDuration = 2.seconds

      override def requestTimeout: Duration = 2.seconds
    })
    val bindingFuture = Http().bindAndHandleSync({
      case HttpRequest(_, _, headers, _, _) =>
        val upgrade = headers.collectFirst { case u: UpgradeToWebSocket => u }.get
        upgrade.handleMessages(Flow.fromSinkAndSource(Sink.ignore, Source.fromPublisher(source)), None)
    }, interface = "localhost", port = 0)

    val binding = Await.result(bindingFuture, 2.seconds)
    val myPort = binding.localAddress.getPort

    val settings = BlockchainSettings(s"ws://localhost:$myPort", "blockchain" + System.currentTimeMillis(), Set.empty, keepAlive = 1.seconds)
    val manager = new BlockchainManager(settings)

    manager.start()

    Thread.sleep(1000)
    val structs = for {
      i <- 1 to 6
      json = scala.io.Source.fromFile(getClass.getResource(s"/transactions/transaction$i.json").toURI.getPath).mkString
    } yield {
      source.sendNext(TextMessage.Strict(json))
      JacksonJson.fromJson[BlockchainMessage](json).x.map(_.toStruct()).get
    }
    source.sendComplete()

    structs.size shouldBe 6

    Thread.sleep(2000)

    val records = manager.get()
    Await.ready(binding.unbind(), 3.seconds)

    records.size() shouldBe 6
    records.foreach { record =>
      record.kafkaPartition() shouldBe 0
      record.topic() shouldBe settings.kafkaTopic
      record.valueSchema() shouldBe Transaction.ConnectSchema
    }

    records.map(_.value().asInstanceOf[Struct]) shouldBe structs

    materializer.shutdown()
    system.terminate()
  }

}
