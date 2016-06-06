/**
  * Copyright 2016 Datamountaineer.
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
  **/

package com.datamountaineer.streamreactor.socketstreamer.routes

import com.datamountaineer.streamreactor.socketstreamer.domain.KafkaRequestProps
import com.datamountaineer.streamreactor.socketstreamer.flows.KafkaFlow
import akka.http.scaladsl.server.{Directives, Route}
import de.heikoseeberger.akkasse.EventStreamMarshalling
import Directives._
import EventStreamMarshalling._


trait SocketRoutes extends KafkaFlow {
  val topicsString = "topics"
  val webSocketPathString = "ws"
  val serverSendEventsPathString = "sse"
  val params = parameters('topic.as[String], 'consumergroup.as[String], 'readfromend.as[String] ? false).as(KafkaRequestProps)

  def mainFlow() : Route = {

    def webSocketStream(kafkaRequestProps: KafkaRequestProps) : Route  = {
      handleWebSocketMessages(webSocketFlow(kafkaRequestProps))
    }

    def serverSendEvent(kafkaRequestProps: KafkaRequestProps) : Route = {
      complete(serverSendFlow(kafkaRequestProps))
    }

    //Web socket route
    pathPrefix(webSocketPathString) {
      pathPrefix(topicsString) {
        get {
          params { kafkaRequestProps => webSocketStream(kafkaRequestProps)}
        }
      }
    } ~
    pathPrefix(serverSendEventsPathString) {
      pathPrefix(topicsString) {
        get {
          params { kafkaRequestProps => serverSendEvent(kafkaRequestProps)}
        }
      }
    }
  }
}
