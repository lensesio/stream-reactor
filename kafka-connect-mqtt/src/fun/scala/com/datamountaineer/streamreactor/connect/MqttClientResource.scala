package com.datamountaineer.streamreactor.connect

import cats.effect.IO
import cats.effect.Resource
import com.typesafe.scalalogging.LazyLogging
import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

import java.util.UUID
import scala.collection.mutable

object MqttClientResource {

  class MqttMessageCache extends MqttCallback with LazyLogging {

    private val queue: mutable.Queue[MqttMessage] = mutable.Queue()

    override def messageArrived(topic: String, message: MqttMessage): Unit = {
      logger.info(s"Received message on topic $topic")
      queue.addOne(message)
    }

    override def deliveryComplete(token: IMqttDeliveryToken): Unit = {}

    override def connectionLost(cause: Throwable): Unit = {}

    def latestPayloadAsString: Option[String] =
      queue.headOption.map(q => new String(q.getPayload))
  }
  def apply(url: String, user: String, password: String, topic: String): Resource[IO, (() => Option[String])] = {

    def createMqttConnectOptions(): MqttConnectOptions = {
      val conOpt = new MqttConnectOptions
      conOpt.setCleanSession(true)
      conOpt.setUserName(user)
      conOpt.setPassword(password.toCharArray)
      conOpt
    }

    def createMqttClient(
      conOpt:    MqttConnectOptions,
      dataStore: MqttClientPersistence,
      callback:  MqttCallback,
    ) = {
      val client = new MqttClient(url, UUID.randomUUID().toString, dataStore)
      client.setCallback(callback)
      client.connect(conOpt)
      client.subscribe(topic)
      client
    }

    for {
      dataStore         <- Resource.fromAutoCloseable(IO(new MemoryPersistence()))
      mqttConnectOptions = createMqttConnectOptions()
      messageCache       = new MqttMessageCache()
      _                 <- Resource.fromAutoCloseable(IO(createMqttClient(mqttConnectOptions, dataStore, messageCache)))
    } yield (() => messageCache.latestPayloadAsString)
  }

}
