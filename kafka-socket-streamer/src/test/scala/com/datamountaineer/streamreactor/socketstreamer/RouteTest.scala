package com.datamountaineer.streamreactor.socketstreamer

import akka.actor.ActorSystem
import akka.http.scaladsl.testkit.{ScalatestRouteTest, WSProbe}
import com.datamountaineer.streamreactor.socketstreamer.avro.{BinaryDecoder, StringDecoder}
import com.datamountaineer.streamreactor.socketstreamer.routes.KafkaSocketRoutes
import org.scalatest._

/**
  * Created by andrew@datamountaineer.com on 16/03/16. 
  * stream-reactor-websocket-feeder
  */
class RouteTest extends FlatSpec with Matchers with ScalatestRouteTest {
  it should "handle websocket requests for topics" in {
    implicit val system = ActorSystem("bibble")
    implicit val config = SocketStreamerConfig("bibble", "localhost:2181", "localhost:9092", "http://localhost:8081", 8787)
    implicit val kafkaAvroDecoder = KafkaAvroDecoderFn(config)
    val wsClient = WSProbe()
    WS("/api/kafka/ws?query=SELECT+%2A+FROM+test+WITHFORMAT+AVRO+WITHGROUP+123", wsClient.flow) ~> KafkaSocketRoutes(system, config, kafkaAvroDecoder, StringDecoder, BinaryDecoder).routes ~> check {
      isWebSocketUpgrade shouldEqual true
      wsClient.expectMessage("")
    }
  }

  //  it should "handle server send requests for topics" in {
  //    implicit val routeTestTimeout = RouteTestTimeout(10 seconds)
  //    Get("/sse/topics?topic=test&consumergroup=1234") ~> mainFlow() ~> check {
  //     status.isSuccess() should be (true)
  //     val resp = response
  //    }
  //  }
}
