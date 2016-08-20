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

import akka.actor.ActorSystem
import akka.http.scaladsl.server.{Directives, Route, ValidationRejection}
import com.datamountaineer.streamreactor.socketstreamer.SocketStreamerConfig
import com.datamountaineer.streamreactor.socketstreamer.domain.KafkaStreamingProps
import com.datamountaineer.streamreactor.socketstreamer.flows.KafkaFlow
import io.confluent.kafka.serializers.KafkaAvroDecoder
import de.heikoseeberger.akkasse.EventStreamMarshalling
import Directives._
import EventStreamMarshalling._
import com.typesafe.scalalogging.slf4j.StrictLogging
import kafka.serializer.Decoder

import scala.util.{Failure, Success, Try}


object KafkaSocketRoutes extends StrictLogging {

  def apply()(implicit system: ActorSystem,
              config: SocketStreamerConfig,
              kafkaDecoder: KafkaAvroDecoder): Route = {

    pathPrefix("api" / "kafka") {
      pathPrefix("ws") {
        get {
          parameter('query) { query =>
            implicit val decoder:Decoder[AnyRef] = kafkaDecoder
            withKafkaStreamingProps(query) { props =>
              handleWebSocketMessages(KafkaFlow.createWebSocketFlow(props))
            }
          }
        }
      } ~
        path("sse") {
          get {
            parameter('query) { query =>
              implicit val decoder:Decoder[AnyRef] = kafkaDecoder
              withKafkaStreamingProps(query) { props =>
                complete(KafkaFlow.createServerSendFlow(props))
              }
            }
          }
        }
    }
  }

  private def withKafkaStreamingProps(query: String)(thunk: KafkaStreamingProps => Route) = {
    Try(KafkaStreamingProps(query)) match {
      case Failure(t) =>
        reject(ValidationRejection(s"Invalid query:$query. ${t.getMessage}"))
      case Success(prop) => thunk(prop)
    }
  }
}
