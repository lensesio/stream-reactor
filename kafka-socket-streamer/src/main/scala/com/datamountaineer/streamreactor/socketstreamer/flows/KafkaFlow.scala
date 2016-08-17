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

package com.datamountaineer.streamreactor.socketstreamer.flows

import akka.actor.ActorSystem
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.datamountaineer.streamreactor.socketstreamer.ConsumerRecordHelper._
import com.datamountaineer.streamreactor.socketstreamer.SocketStreamerConfig
import com.datamountaineer.streamreactor.socketstreamer.domain._
import com.typesafe.scalalogging.slf4j.StrictLogging
import de.heikoseeberger.akkasse.ServerSentEvent
import io.confluent.kafka.serializers.KafkaAvroDecoder
import spray.json._

import scala.concurrent.duration._
import scala.language.postfixOps

object KafkaFlow extends StrictLogging with HeartBeatMessageFormat {

  /**
    * Create a flow with a null sink (don't accept incoming websocket data) and a source from Kafka out to the websocket
    *
    * @param props A KafkaStreamingProps to use to create a KafkaConsumer
    * @return a Flow with a null inbound sink and a ReactiveKafka publisher source
    **/
  def createWebSocketFlow(props: KafkaStreamingProps)
                         (implicit system: ActorSystem, config: SocketStreamerConfig, kafkaAvroDecoder: KafkaAvroDecoder): Flow[Message, Message, Any] = {
    logger.info("Establishing flow")
    val kafkaSource = KafkaSourceCreateFn(props)
    implicit val extractor = props.fieldsValuesExtractor
    val flow: Flow[Message, Message, Any] = Flow
      .fromSinkAndSource(Sink.ignore, kafkaSource.map(_.toWSMessage))
      .keepAlive(1.second, () => TextMessage.Strict(""))
    flow
  }

  /**
    * Create one directional flow of ServerSendEvents
    *
    * @param props A KafkaRequestProps to use to create a KafkaConsumer
    * @return a ReactiveKafka publisher source
    **/
  def createServerSendFlow(props: KafkaStreamingProps)
                          (implicit system: ActorSystem, config: SocketStreamerConfig, kafkaAvroDecoder: KafkaAvroDecoder): Source[ServerSentEvent, Control] = {
    logger.info("Establishing Send Server Event stream.")
    implicit val extractor = props.fieldsValuesExtractor
    //get the kafka source
    val source = KafkaSourceCreateFn(props)
      .map { record =>
        val sse = record.toSSEMessage
        sse
      }
      .keepAlive(1.second, () => ServerSentEvent(HeartBeatMessage(system.name).toJson.compactPrint))

    source
  }

  implicit def convert(props: KafkaStreamingProps): KafkaClientProps = {
    KafkaClientProps(props.topic, props.group, props.partitionOffset)
  }
}
