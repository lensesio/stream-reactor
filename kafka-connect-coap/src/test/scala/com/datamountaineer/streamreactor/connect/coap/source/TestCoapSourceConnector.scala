/*
 * Copyright 2017 Datamountaineer.
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
 */

package com.datamountaineer.streamreactor.connect.coap.source

import com.datamountaineer.streamreactor.connect.coap.TestBase
import com.datamountaineer.streamreactor.connect.coap.configs.CoapConstants
import org.scalatest.WordSpec

import scala.collection.JavaConversions._

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
    taskConfigs.head.get(CoapConstants.COAP_KCQL) shouldBe SOURCE_KCQL_SECURE
    taskConfigs.head.get(CoapConstants.COAP_KEY_STORE_PATH) shouldBe KEYSTORE_PATH
    taskConfigs.head.get(CoapConstants.COAP_TRUST_STORE_PATH) shouldBe TRUSTSTORE_PATH
    taskConfigs.head.get(CoapConstants.COAP_URI) shouldBe SOURCE_URI_SECURE
    connector.taskClass() shouldBe classOf[CoapSourceTask]
  }

  "should create a CoapSourceConnector multiple kcql" in {
    val props = getPropsSecureMultipleKCQL
    val connector = new CoapSourceConnector
    connector.start(props)
    val taskConfigs = connector.taskConfigs(2)
    taskConfigs.size() shouldBe 2
    taskConfigs.head.get(CoapConstants.COAP_KCQL) shouldBe SOURCE_KCQL_SECURE
    taskConfigs.head.get(CoapConstants.COAP_KEY_STORE_PATH) shouldBe KEYSTORE_PATH
    taskConfigs.head.get(CoapConstants.COAP_TRUST_STORE_PATH) shouldBe TRUSTSTORE_PATH
    taskConfigs.head.get(CoapConstants.COAP_URI) shouldBe SOURCE_URI_SECURE
    connector.taskClass() shouldBe classOf[CoapSourceTask]
  }
}
