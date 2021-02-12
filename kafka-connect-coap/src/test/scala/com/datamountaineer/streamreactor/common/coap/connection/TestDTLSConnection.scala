package com.datamountaineer.streamreactor.common.coap.connection



import com.datamountaineer.streamreactor.common.coap.TestBase
import com.datamountaineer.streamreactor.common.coap.configs.{CoapSettings, CoapSinkConfig}
import org.eclipse.californium.core.CoapClient
import org.scalatest.BeforeAndAfter

/**
  * Created by andrew@datamountaineer.com on 25/08/2017. 
  * stream-reactor
  */
class TestDTLSConnection extends TestBase with BeforeAndAfter {

//  val server = new Server(SINK_PORT_SECURE, SINK_PORT_INSECURE, kEY_PORT_SECURE)
//
//  before { server.start() }
//  after { server.stop() }
//
//  CaliforniumLogger.initialize()
//  CaliforniumLogger.setLevel(Level.INFO)
//  ScandiumLogger.initialize()
//  ScandiumLogger.setLevel(Level.INFO)

  "should create a DTSConnection with Public/Private PEM keys" in {
    val props = getPropsSecurePEM
    val config = CoapSinkConfig(props)
    val settings = CoapSettings(config)
    val client = DTLSConnectionFn(settings.head)
    client.isInstanceOf[CoapClient] shouldBe true
  }

  "should create a DTSConnection with PSK" in {
    val props = getPropsSecurePSK
    val config = CoapSinkConfig(props)
    val settings = CoapSettings(config)
    val client = DTLSConnectionFn(settings.head)
    client.isInstanceOf[CoapClient] shouldBe true
  }
}
