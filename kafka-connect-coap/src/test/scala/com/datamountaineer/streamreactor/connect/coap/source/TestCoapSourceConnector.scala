package com.datamountaineer.streamreactor.connect.coap.source

import com.datamountaineer.streamreactor.connect.coap.TestBase
import com.datamountaineer.streamreactor.connect.coap.configs.CoapSourceConfig
import org.scalatest.WordSpec

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 28/12/2016. 
  * stream-reactor
  */
class TestCoapSourceConnector extends WordSpec with TestBase {
  "should create a CoapSourceConnector" in {
    val props = getPropsSecure
    val connector = new CoapSourceConnector
    connector.start(props)
    val taskConfigs = connector.taskConfigs(2)
    taskConfigs.size() shouldBe 1
    taskConfigs.asScala.head.get(CoapSourceConfig.COAP_KCQL) shouldBe KCQL_SECURE
    taskConfigs.asScala.head.get(CoapSourceConfig.COAP_KEY_STORE_PATH) shouldBe KEYSTORE_PATH
    taskConfigs.asScala.head.get(CoapSourceConfig.COAP_TRUST_STORE_PATH) shouldBe TRUSTSTORE_PATH
    taskConfigs.asScala.head.get(CoapSourceConfig.COAP_URI) shouldBe URI_SECURE
    connector.taskClass() shouldBe classOf[CoapSourceTask]
  }

  "should create a CoapSourceConnector multiple kcql" in {
    val props = getPropsSecureMultipleKCQL
    val connector = new CoapSourceConnector
    connector.start(props)
    val taskConfigs = connector.taskConfigs(2)
    taskConfigs.size() shouldBe 2
    taskConfigs.asScala.head.get(CoapSourceConfig.COAP_KCQL) shouldBe KCQL_SECURE
    taskConfigs.asScala.head.get(CoapSourceConfig.COAP_KEY_STORE_PATH) shouldBe KEYSTORE_PATH
    taskConfigs.asScala.head.get(CoapSourceConfig.COAP_TRUST_STORE_PATH) shouldBe TRUSTSTORE_PATH
    taskConfigs.asScala.head.get(CoapSourceConfig.COAP_URI) shouldBe URI_SECURE
    connector.taskClass() shouldBe classOf[CoapSourceTask]
  }
}
