/**
  * Copyright 2016 Datamountaineer.
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
  **/

package com.datamountaineer.streamreactor.connect.blockchain.config

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

object BlockchainConfig {
  val CONNECTION_URL = "connect.blockchain.source.url"
  private val CONNECTION_URL_DOC = "The websocket connection."
  private val CONNECTION_URL_DEFAULT = "wss://ws.blockchain.info/inv"

  val ADDRESS_SUBSCRIPTION = "connect.blockchain.source.subscription.addresses"
  private val ADDRESS_SUBSCRIPTION_DOC = "Comma separated list of addresses to receive transactions updates for"

  val KAFKA_TOPIC = "connect.blockchain.source.kafka.topic"
  val KAFKA_TOPIC_DOC = "Specifies the kafka topic to sent the records to."

  val config: ConfigDef = new ConfigDef()
    .define(CONNECTION_URL, Type.STRING, CONNECTION_URL_DEFAULT, Importance.HIGH, CONNECTION_URL_DOC)
    .define(ADDRESS_SUBSCRIPTION, Type.STRING, null, Importance.LOW, ADDRESS_SUBSCRIPTION_DOC)
    .define(KAFKA_TOPIC, Type.STRING, Importance.HIGH, KAFKA_TOPIC_DOC)
}

case class BlockchainConfig(props: util.Map[String, String]) extends AbstractConfig(BlockchainConfig.config, props)
