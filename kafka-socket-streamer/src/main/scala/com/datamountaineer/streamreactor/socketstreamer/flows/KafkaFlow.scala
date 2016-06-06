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

import java.io.Serializable
import java.util.{Calendar, Properties}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.ws.TextMessage.Strict
import spray.json._
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.datamountaineer.streamreactor.socketstreamer.ConfigurationLoader
import com.datamountaineer.streamreactor.socketstreamer.domain._
import com.softwaremill.react.kafka.{ConsumerProperties, ReactiveKafka}
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.confluent.kafka.serializers.KafkaAvroDecoder
import kafka.utils.VerifiableProperties
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.reactivestreams.Publisher

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import de.heikoseeberger.akkasse.ServerSentEvent

import scala.language.postfixOps

trait KafkaFlow extends KafkaConstants with ConfigurationLoader with StrictLogging with Protocols {
  implicit def executor: ExecutionContextExecutor
  val decoder = getDecoder

  def getDecoder : KafkaAvroDecoder = {
    val props = new Properties()
    props.put(ZOOKEEPER_KEY, zookeepers)
    props.put(SCHEMA_REGISTRY_URL, schemaRegistryUrl)
    val vProps = new VerifiableProperties(props)
    new KafkaAvroDecoder(vProps)
  }


  /**
    * Create a flow with a null sink (don't accept incoming websocket data) and a source from Kafka out to the websocket
    *
    *  @param kafkaRequestProps A KafkaRequestProps to use to create a KafkaConsumer
    *  @return a Flow with a null inbound sink and a ReactiveKafka publisher source
    * */
  def webSocketFlow(kafkaRequestProps: KafkaRequestProps) : Flow[Message, Message, Any] = {
    implicit val actorSystem = ActorSystem(systemName)
    val kafkaSource = createKafkaSource(kafkaRequestProps)
    logger.info("Establishing flow")
    val flow = Flow.fromSinkAndSource(Sink.ignore, kafkaSource map toWSMessage)
                    .keepAlive(1.second, () => TextMessage.Strict(""))
    flow
  }

  /**
    * Convert ConsumerRecord to string json payload.
    *
    * @param consumerRecord A consumerRecord to convert to TextMessage
    * @return A TextMessage with a WebMessage Json string
    * */
  def toWSMessage(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]) : Strict = {
    val message = convertToJsonString(consumerRecord)
    TextMessage.Strict(message)
  }

  /**
    * Convert the Kafka consumer record to a json string
    *
    * @param consumerRecord The Kafka consumer record to convert
    * @return A Json string representing a StreamMessage
    * */
  def convertToJsonString(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]) : String = {
    //using confluent's decoder
    val key = if (consumerRecord.key() == null) None else Some(decoder.fromBytes(consumerRecord.key()).toString)
    val payload = if (consumerRecord.value() == null) None else  Some(decoder.fromBytes(consumerRecord.value()).toString)
    StreamMessage(key, payload).toJson.compactPrint
  }

  /**
    * Create one directional flow of ServerSendEvents
    *
    *  @param kafkaRequestProps A KafkaRequestProps to use to create a KafkaConsumer
    *  @return a ReactiveKafka publisher source
    * */
  def serverSendFlow(kafkaRequestProps: KafkaRequestProps) = {
    implicit val actorSystem = ActorSystem(systemName)
    implicit val mat = ActorMaterializer()

    //get the kafka source
    val kafkaSource = createKafkaSource(kafkaRequestProps)
    logger.info("Establishing Send Server Event stream.")

    //establish the kafka stream
    val source = kafkaSource
                  .map(m=> toSSEMessage(m))
                  .keepAlive(1.second, () => ServerSentEvent(heartBeatMessage))

    //complete the request to start the stream
    source
  }

  /**
    * Convert a ConsumerRecord to a ServerSendEvent
    *
    * @param consumerRecord A ConsumerRecord to convert
    * @return A ServerSentEvent
    * */
  def toSSEMessage(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]) : ServerSentEvent = {
    val message = convertToJsonString(consumerRecord)
    ServerSentEvent(message)
  }

  /**
    * Create a heartbeat message for a topic
    *
    * @return A string for the heartbeat message
    * */
  def heartBeatMessage  : String = {
    val message = HeartBeatMessage(Calendar.getInstance.getTime.toString, systemName, "heartbeat")
    message.toJson.compactPrint
  }

  /**
    * Create a Kafka source
    *
    * @param kafkaRequestProps A KafkaRequestProps to use to create a KafkaConsumer
    * @return A Source of [ConsumerRecord, Unit]
    * */
  def createKafkaSource(kafkaRequestProps: KafkaRequestProps) : Source[ConsumerRecord[Array[Byte], Array[Byte]], NotUsed] = {
    implicit val actorSystem = ActorSystem(systemName)
    implicit val mat = ActorMaterializer()
    logger.info(s"Setting up Kafka consumer properties for topic ${kafkaRequestProps.topic}")
    val consumerProps = ConsumerProperties(bootstrapServers = kafkaBootstrapServers,
      topic = kafkaRequestProps.topic,
      groupId = kafkaRequestProps.consumerGroup,
      keyDeserializer = new ByteArrayDeserializer,
      valueDeserializer = new ByteArrayDeserializer
    ).commitInterval(600 milliseconds)

    //if set for new consumer groups only read from the end of the stream .i.e new messages published to the topic
    if (kafkaRequestProps.readFromEnd) consumerProps.readFromEndOfStream()

    consumerProps.dump

    //Set up kafka consumer as a publisher
    val pub: Publisher[ConsumerRecord[Array[Byte], Array[Byte]]] = new ReactiveKafka().consume(consumerProps)
    Source.fromPublisher(pub)
  }
}
