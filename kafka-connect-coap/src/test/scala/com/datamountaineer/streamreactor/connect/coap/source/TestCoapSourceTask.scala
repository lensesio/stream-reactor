package com.datamountaineer.streamreactor.connect.coap.source

import akka.actor.ActorSystem
import com.datamountaineer.streamreactor.connect.coap.{DTLSConnectionFn, Server, TestBase}
import akka.util.Timeout
import com.datamountaineer.streamreactor.connect.coap.configs.{CoapSourceConfig, CoapSourceSettings}
import org.apache.kafka.connect.data.Struct
import org.eclipse.californium.core.CoapClient
import akka.pattern.ask
import akka.util.Timeout
import org.eclipse.californium.core.network.CoapEndpoint
import org.eclipse.californium.core.network.config.NetworkConfig

import scala.concurrent.duration._
import org.scalatest.{BeforeAndAfter, WordSpec}

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 28/12/2016. 
  * stream-reactor
  */
class TestCoapSourceTask extends WordSpec with BeforeAndAfter with TestBase {
    val server = new Server()

    before {
      server.start()
    }

    after {
      server.stop()
    }

//  "should create a secure reader and read a message" in {
//    implicit val system = ActorSystem()
//    implicit val timeout = Timeout(60 seconds)
//    val props = getPropsSecure
//    val config = CoapSourceConfig(props)
//    val settings = CoapSourceSettings(config)
//    val actorProps = CoapReader(settings)
//    val reader = system.actorOf(actorProps.head._2, actorProps.head._1)
//    reader ? StartChangeFeed
//    val client = new CoapClient()
//    client.setEndpoint(new CoapEndpoint(DTLSConnectionFn(settings.settings.head), NetworkConfig.getStandard()))
//    client.setURI(s"$URI_SECURE/$RESOURCE_SECURE")
//    client.post("Message1", 0)
//    Thread.sleep(2000)
//    val records = ActorHelper.askForRecords(reader)
//    records.size() shouldBe 1
//    val record = records.asScala.head
//    val struct = record.value().asInstanceOf[Struct]
//    struct.getString("payload") shouldBe "Message1"
//    struct.getString("type") shouldBe "ACK"
//    reader ? StopChangeFeed
//  }

  "should create a task" in {
      implicit val system = ActorSystem()
      implicit val timeout = Timeout(60 seconds)
      val props = getPropsUnsecure
      val task = new CoapSourceTask()
      task.start(props)
      Thread.sleep(1000)
      val records = task.poll()
      records.size() shouldBe 0

      val client = new CoapClient(s"$URI_INSECURE/$RESOURCE_INSECURE")
      client.post("Message1", 0)
      Thread.sleep(3000)
      val records2 = task.poll()
      records2.size() shouldBe 1
      val record2 = records2.asScala.head
      val struct2 = record2.value().asInstanceOf[Struct]
      struct2.getString("payload") shouldBe "Message1"
      struct2.getString("type") shouldBe "ACK"

      client.post("Message2", 0)
      Thread.sleep(3000)
      val records3 = task.poll()
      records3.size() shouldBe 1
      val record3 = records3.asScala.head
      val struct3 = record3.value().asInstanceOf[Struct]
      struct3.getString("payload") shouldBe "Message2"
      struct3.getString("type") shouldBe "ACK"
      task.stop()
  }
}
