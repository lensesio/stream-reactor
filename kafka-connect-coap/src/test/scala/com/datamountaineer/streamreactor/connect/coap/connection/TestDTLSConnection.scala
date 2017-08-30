package com.datamountaineer.streamreactor.connect.coap.connection


import java.util.logging.Level
import javax.ws.rs.core.MediaType

import com.datamountaineer.streamreactor.connect.coap.{Server, TestBase}
import com.datamountaineer.streamreactor.connect.coap.configs.{CoapSettings, CoapSinkConfig}
import org.eclipse.californium.core.{CaliforniumLogger, CoapClient}
import org.eclipse.californium.scandium.ScandiumLogger
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
