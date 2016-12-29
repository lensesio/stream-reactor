package com.datamountaineer.streamreactor.connect.coap.config

import com.datamountaineer.streamreactor.connect.coap.TestBase
import com.datamountaineer.streamreactor.connect.coap.configs.{CoapSourceConfig, CoapSourceSettings}
import org.apache.kafka.common.config.ConfigException
import org.scalatest.WordSpec

/**
  * Created by andrew@datamountaineer.com on 28/12/2016. 
  * stream-reactor
  */
class TestCoapSourceSettings extends WordSpec with TestBase {
  "should create CoapSettings for an insecure connection" in {
    val props = getPropsUnsecure
    val config = CoapSourceConfig(props)
    val settings = CoapSourceSettings(config)
    val setting = settings.head
    setting.kcql.getSource shouldBe RESOURCE_INSECURE
    setting.kcql.getTarget shouldBe TOPIC
    setting.uri shouldBe URI_INSECURE
    setting.keyStoreLoc.nonEmpty shouldBe false
    setting.trustStoreLoc.nonEmpty shouldBe false
  }

  "should create CoapSettings for an secure connection" in {
    val props = getPropsSecure
    val config = CoapSourceConfig(props)
    val settings = CoapSourceSettings(config)
    val setting = settings.head
    setting.kcql.getSource shouldBe RESOURCE_SECURE
    setting.kcql.getTarget shouldBe TOPIC
    setting.uri shouldBe URI_SECURE
    setting.keyStoreLoc.nonEmpty shouldBe true
    setting.trustStoreLoc.nonEmpty shouldBe true
  }

  "should fail to create CoapSettings for an secure connection with key wrong path" in {
    val props = getPropsSecureKeyNotFound
    val config = CoapSourceConfig(props)
    intercept[ConfigException] {
      CoapSourceSettings(config)
    }
  }

  "should fail to create CoapSettings for an secure connection with trust wrong path" in {
    val props = getPropsSecureTrustNotFound
    val config = CoapSourceConfig(props)
    intercept[ConfigException] {
      CoapSourceSettings(config)
    }
  }
}
