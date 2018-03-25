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

import java.util

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Terminated}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest}
import akka.pattern.ask
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, StreamTcpException}
import com.datamountaineer.streamreactor.connect.blockchain.config.BlockchainSettings
import com.datamountaineer.streamreactor.connect.blockchain.data.BlockchainMessage
import com.datamountaineer.streamreactor.connect.blockchain.json.JacksonJson
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.SourceRecord

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success, Try}

class BlockchainManager(settings: BlockchainSettings) extends AutoCloseable with StrictLogging {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  private val bufferActorRef = BufferActor()
  private var cancelFlow: Option[Promise[Option[Message]]] = None

  import system._

  def start(): Future[Done] = {
    try {
      createFlow(bufferActorRef)
    }
    catch {
      case tcpException: StreamTcpException =>
        logger.error(s"Connection to ${settings.url} could not be established. Stopping BlockchainManager...", tcpException)
        Await.ready(system.terminate(), 1.minute)
        throw tcpException
      case t: Throwable =>
        logger.error(s"Could not start due to ${t.getMessage}. Stopping BlockchainManager...", t)
        Await.ready(system.terminate(), 1.minute)
        throw t
    }
  }

  def stop(): Future[Terminated] = {
    require(cancelFlow.isDefined, "The blockchain manager hasn't been started yet")
    cancelFlow.foreach(_.success(None))

    Await.ready(system.terminate(), 1.minute)
  }

  def close(): Unit = stop()

  def get(): util.ArrayList[SourceRecord] = {
    implicit val timeout = akka.util.Timeout(10.seconds)
    Await.result((bufferActorRef ? BufferActor.DataRequest)
      .map(_.asInstanceOf[util.ArrayList[SourceRecord]])
      .recoverWith { case t =>
        logger.error("Could not retrieve the source records", t)
        Future.successful(new util.ArrayList[SourceRecord]())
      }, Duration.Inf)
  }

  private def createFlow(buffer: ActorRef) = {
    val incoming: Sink[String, Future[Done]] = {

      Sink.foreach[String] { msg =>
        Try(JacksonJson.fromJson[BlockchainMessage](msg))
          .map(_.x) match {
          case Success(transaction) =>
            transaction.foreach { tx =>
              val sourceRecord = tx.toSourceRecord(settings.kafkaTopic, 0, None)
              buffer ! sourceRecord
            }
          case Failure(t) =>
            logger.warn(s"Could not process message $msg", t)
        }
      }
    }

    val outgoing = Source.single(TextMessage.Strict("{\"op\":\"unconfirmed_sub\"}"))
      .concatMat(Source.maybe[Message])(Keep.right)

    val webSocketFlow = Http().webSocketClientFlow(WebSocketRequest(settings.url))

    val (((_, upgradeResponse), cancellable), closed) =
      outgoing
        .keepAlive(settings.keepAlive, () => TextMessage.Strict("{\"op\":\"ping\"}"))
        .viaMat(webSocketFlow)(Keep.both)
        .concatMat(Source.maybe[Message])(Keep.both)
        .collect {
          case message: TextMessage.Strict => Future.successful(message.text)
          case stream: TextMessage.Streamed => stream.textStream.runFold("")(_ + _).flatMap(c => Future.successful(c))
        }
        .mapAsync(4)(identity)
        .toMat(incoming)(Keep.both)
        .run()

    val response = Await.result(upgradeResponse, settings.openConnectionTimeout).response
    if (response.status != StatusCodes.SwitchingProtocols) {
      sys.error(s"Connection to ${settings.url} failed with ${response.status}")
    }
    cancelFlow = Some(cancellable)
    closed
  }

  object BufferActor {

    case object DataRequest

    def apply(): ActorRef = system.actorOf(Props(new BufferActor), name = "BufferActor")
  }

  class BufferActor() extends Actor with ActorLogging {
    private var buffer = new util.ArrayList[SourceRecord]

    override def receive: Receive = {
      case t: SourceRecord =>
        buffer.add(t)
      case BufferActor.DataRequest =>
        sender() ! buffer
        buffer = new util.ArrayList[SourceRecord]
    }
  }

}

