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
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Route, ValidationRejection}
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerSettings, KafkaConsumerActor, Subscriptions}
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.datamountaineer.streamreactor.socketstreamer.ConsumerRecordHelper._
import com.datamountaineer.streamreactor.socketstreamer.avro.GenericRecordFieldsValuesExtractor
import com.datamountaineer.streamreactor.socketstreamer.domain.{HeartBeatMessage, KafkaClientProps, KafkaStreamingProps}
import com.datamountaineer.streamreactor.socketstreamer.flows.KafkaSourceCreateFn
import com.datamountaineer.streamreactor.socketstreamer.{JacksonJson, SocketStreamerConfig}
import com.typesafe.scalalogging.slf4j.StrictLogging
import de.heikoseeberger.akkasse.{EventStreamMarshalling, ServerSentEvent}
import io.confluent.kafka.serializers.KafkaAvroDecoder
import kafka.serializer.Decoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import EventStreamMarshalling._
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

case class KafkaSocketRoutes(system: ActorSystem,
                             config: SocketStreamerConfig,
                             kafkaDecoder: KafkaAvroDecoder,
                             textDcoder: Decoder[AnyRef],
                             binaryDecoder: Decoder[AnyRef]) extends StrictLogging {

  private implicit val actorSystem = system
  private implicit val socketStreamConfig = config

  private val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new ByteArrayDeserializer)
    .withBootstrapServers(config.kafkaBrokers)
    .withGroupId("datamountaineer")
    //if an offset is out of range or the offset doesn't exist yet default to earliest available
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  private val schemasConsumerRef = system.actorOf(KafkaConsumerActor.props(consumerSettings))

  private val schemaSource = Consumer
    .plainExternalSource[Array[Byte], Array[Byte]](schemasConsumerRef, Subscriptions.assignmentWithOffset(new TopicPartition("_schemas", 0), 0))

  private lazy val webSocketFlowForSchemasTopic: Flow[Message, Message, Any] = {
    implicit val extractor = GenericRecordFieldsValuesExtractor(true, Map.empty)
    implicit val decoder = textDcoder

    Flow.fromSinkAndSource(Sink.ignore, schemaSource.map(_.toWSMessage()))
      .keepAlive(1.second, () => TextMessage.Strict(""))
  }

  private lazy val sseSocketFlowForSchemasTopic: Source[ServerSentEvent, Any] = {
    implicit val extractor = GenericRecordFieldsValuesExtractor(true, Map.empty)
    implicit val decoder = textDcoder

    schemaSource
      .map(_.toSSEMessage())
      .keepAlive(1.second, () => ServerSentEvent(JacksonJson.toJson(HeartBeatMessage(system.name))))
  }

  val routes: Route = {
    pathPrefix("api" / "kafka") {
      pathPrefix("ws") {
        get {
          parameter('query) { query =>
            withKafkaStreamingProps(query) { props =>
              props.topic.toLowerCase() match {
                case "_schemas" =>
                  handleWebSocketMessages(webSocketFlowForSchemasTopic)
                case _ => handleWebSocketMessages(createWebSocketFlow(props))
              }
            }
          }
        }
      } ~
        path("sse") {
          get {
            parameter('query) { query =>
              implicit val decoder: Decoder[AnyRef] = kafkaDecoder
              withKafkaStreamingProps(query) { props =>
                props.topic.toLowerCase() match {
                  case "_schemas" => complete(sseSocketFlowForSchemasTopic)
                  case _ => complete(createServerSendFlow(props))
                }
              }
            }
          }
        }
    }
  }

  private def withKafkaStreamingProps(query: String)(thunk: KafkaStreamingProps => Route) = {
    Try(KafkaStreamingProps(query)(kafkaDecoder, textDcoder, binaryDecoder)) match {
      case Failure(t) =>
        reject(ValidationRejection(s"Invalid query:$query. ${t.getMessage}"))
      case Success(prop) => thunk(prop)
    }
  }

  /**
    * Create a flow with a null sink (don't accept incoming websocket data) and a source from Kafka out to the websocket
    *
    * @param props A KafkaStreamingProps to use to create a KafkaConsumer
    * @return a Flow with a null inbound sink and a ReactiveKafka publisher source
    **/
  private def createWebSocketFlow(props: KafkaStreamingProps): Flow[Message, Message, Any] = {
    logger.info("Establishing flow")
    val kafkaSource = KafkaSourceCreateFn(props)
    implicit val extractor = props.fieldsValuesExtractor
    implicit val decoder = props.decoder
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
  private def createServerSendFlow(props: KafkaStreamingProps): Source[ServerSentEvent, Control] = {
    logger.info("Establishing Send Server Event stream.")
    implicit val extractor = props.fieldsValuesExtractor
    implicit val decoder = props.decoder
    //get the kafka source
    KafkaSourceCreateFn(props)
      .map(_.toSSEMessage)
      .keepAlive(1.second, () => ServerSentEvent(JacksonJson.toJson(HeartBeatMessage(system.name))))
  }

  implicit def convert(props: KafkaStreamingProps): KafkaClientProps = {
    KafkaClientProps(props.topic, props.group, props.partitionOffset, props.sampleProps)
  }
}
