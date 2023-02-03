package com.datamountaineer.streamreactor.connect

import com.datamountaineer.streamreactor.connect.MqttClientResource.MqttMessageCache
import com.typesafe.scalalogging.LazyLogging
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.client.mqttv3.{IMqttDeliveryToken, MqttCallback, MqttClient, MqttClientPersistence, MqttConnectOptions, MqttMessage}

import java.util.UUID
import scala.collection.mutable
import scala.util.Try

case class MqttClientResource(callback: MqttMessageCache, dataStore: MqttClientPersistence, client: MqttClient) extends AutoCloseable {
  override def close(): Unit = {
    Try(client.close())
    Try(dataStore.close())
    ()
  }

  def latestPayloadAsString: Option[String] = callback.latestPayloadAsString

}

object MqttClientResource {

  class MqttMessageCache extends MqttCallback with LazyLogging {

    private val queue: mutable.Queue[MqttMessage] = mutable.Queue()

    override def messageArrived(topic: String, message: MqttMessage): Unit = {
      logger.info(s"Received message on topic $topic")
      queue.addOne(message)
    }

    override def deliveryComplete(token: IMqttDeliveryToken): Unit = {}

    override def connectionLost(cause: Throwable): Unit = {}

    def latestPayloadAsString : Option[String] = {
      queue.headOption.map(q => new String(q.getPayload))
    }
  }

  val callback = new MqttMessageCache()

  val dataStore = new MemoryPersistence()

  def apply(url: String, user: String, password: String, topic: String): MqttClientResource = {
    val conOpt = new MqttConnectOptions
    conOpt.setCleanSession(true)
    conOpt.setUserName(user)
    conOpt.setPassword(password.toCharArray)

    val client = new MqttClient(url, UUID.randomUUID().toString, dataStore)
    client.setCallback(callback)
    client.connect(conOpt)
    client.subscribe(topic)

    MqttClientResource(callback, dataStore, client)
  }

}
