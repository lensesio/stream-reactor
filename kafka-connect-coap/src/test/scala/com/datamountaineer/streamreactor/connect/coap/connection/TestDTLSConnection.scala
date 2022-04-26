package com.datamountaineer.streamreactor.connect.coap.connection



import com.datamountaineer.streamreactor.connect.coap.TestBase
import com.datamountaineer.streamreactor.connect.coap.configs.{CoapConstants, CoapSettings, CoapSinkConfig}
import org.eclipse.californium.core.CoapClient
import org.scalatest.BeforeAndAfter

import java.util
import scala.jdk.CollectionConverters.MapHasAsJava

/**
  * Created by andrew@datamountaineer.com on 25/08/2017. 
  * stream-reactor
  */
class TestDTLSConnection extends TestBase with BeforeAndAfter {

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

  def getPropsSecurePSK: util.Map[String, String] = {
    Map(
      CoapConstants.COAP_IDENTITY->"andrew",
      CoapConstants.COAP_SECRET->"kebab",
      CoapConstants.COAP_KCQL->SOURCE_KCQL_SECURE,
      CoapConstants.COAP_URI->KEY_URI,
      CoapConstants.COAP_DTLS_BIND_PORT->s"$kEY_PORT_SECURE"
    ).asJava
  }

  def getPropsSecurePEM: util.Map[String, String] = {
    Map(
      CoapConstants.COAP_IDENTITY->"andrew",
      CoapConstants.COAP_SECRET->"kebab",
      CoapConstants.COAP_PRIVATE_KEY_FILE->PRIVATE_KEY_PATH,
      CoapConstants.COAP_PUBLIC_KEY_FILE->PUBLIC_KEY_PATH,
      CoapConstants.COAP_KCQL->SOURCE_KCQL_SECURE,
      CoapConstants.COAP_URI->KEY_URI,
      CoapConstants.COAP_DTLS_BIND_PORT->s"$kEY_PORT_SECURE"
    ).asJava
  }

}
