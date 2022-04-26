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

package com.datamountaineer.streamreactor.connect.coap

import com.datamountaineer.streamreactor.connect.coap.configs.CoapConstants
import org.apache.kafka.common.TopicPartition
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util
import scala.jdk.CollectionConverters.MapHasAsJava

/**
  * Created by andrew@datamountaineer.com on 08/08/16. 
  * stream-reactor
  */
trait TestBase extends AnyWordSpec with BeforeAndAfter with Matchers {
  val TOPIC = "coap_test"
  val RESOURCE_SECURE = "secure"
  val RESOURCE_INSECURE = "insecure"
  val SOURCE_KCQL_INSECURE = s"INSERT INTO $TOPIC SELECT * FROM $RESOURCE_INSECURE"
  val SOURCE_KCQL_SECURE = s"INSERT INTO $TOPIC SELECT * FROM $RESOURCE_SECURE"
  val SINK_KCQL_INSECURE = s"INSERT INTO $RESOURCE_INSECURE SELECT * FROM $TOPIC"
  val SINK_KCQL_SECURE = s"INSERT INTO $RESOURCE_SECURE SELECT * FROM $TOPIC"
  val DTLS_PORT = 5684
  val PORT = 5683
  val KEY_PORT = 5682

  val SOURCE_PORT_SECURE: Int = DTLS_PORT
  val SOURCE_PORT_INSECURE: Int = PORT
  val SINK_PORT_SECURE: Int = DTLS_PORT + 1000
  val SINK_PORT_INSECURE: Int = PORT + 1000
  val KEY_PORT_INSECURE: Int = KEY_PORT
  val kEY_PORT_SECURE: Int = KEY_PORT + 1000
  val DISCOVER_URI = s"coap://${CoapConstants.COAP_DISCOVER_IP4}:$SOURCE_PORT_INSECURE"
  val SOURCE_URI_INSECURE = s"coap://localhost:$SOURCE_PORT_INSECURE"
  val SOURCE_URI_SECURE = s"coaps://localhost:$SOURCE_PORT_SECURE"
  val KEY_URI = s"coaps://localhost:$kEY_PORT_SECURE"

  val SINK_URI_INSECURE = s"coap://localhost:$SINK_PORT_INSECURE"
  val SINK_URI_SECURE = s"coaps://localhost:$SINK_PORT_SECURE"

  val KEYSTORE_PASS = "endPass"
  val TRUSTSTORE_PASS = "rootPass"

  val KEYSTORE_PATH: String =  getClass.getResource("/certs2/keyStore.jks").getPath
  val TRUSTSTORE_PATH: String = getClass.getResource("/certs2/trustStore.jks").getPath
  val PRIVATE_KEY_PATH: String = getClass.getResource("/keys/privatekey-pkcs8.pem").getPath
  val PUBLIC_KEY_PATH: String = getClass.getResource("/keys/publickey.pem").getPath

  protected val PARTITION: Int = 12
  protected val TOPIC_PARTITION: TopicPartition = new TopicPartition(TOPIC, PARTITION)
  protected val ASSIGNMENT: util.Set[TopicPartition] =  new util.HashSet[TopicPartition]
  //Set topic assignments
  ASSIGNMENT.add(TOPIC_PARTITION)

  def getPropsInsecure: util.Map[String, String] = {
    Map(CoapConstants.COAP_KCQL->SOURCE_KCQL_INSECURE,
        CoapConstants.COAP_URI->SOURCE_URI_INSECURE
    ).asJava
  }

  def getPropsSecure: util.Map[String, String] = {
    Map(CoapConstants.COAP_KCQL->SOURCE_KCQL_SECURE,
      CoapConstants.COAP_URI->SOURCE_URI_SECURE,
      CoapConstants.COAP_KEY_STORE_PASS->KEYSTORE_PASS,
      CoapConstants.COAP_KEY_STORE_PATH->KEYSTORE_PATH,
      CoapConstants.COAP_TRUST_STORE_PASS->TRUSTSTORE_PASS,
      CoapConstants.COAP_TRUST_STORE_PATH->TRUSTSTORE_PATH,
      CoapConstants.COAP_TRUST_CERTS->"root",
      CoapConstants.COAP_DTLS_BIND_PORT->"63368"
    ).asJava
  }

}


