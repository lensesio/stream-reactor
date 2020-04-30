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

package com.datamountaineer.streamreactor.connect.blockchain.config


/**
  * Holds the constants used in the config.
  **/

object BlockchainConfigConstants {
  val CONNECTION_URL = "connect.blockchain.source.url"
  private[config] val CONNECTION_URL_DOC = "The websocket connection."
  private[config] val CONNECTION_URL_DEFAULT = "wss://ws.blockchain.info/inv"

  val ADDRESS_SUBSCRIPTION = "connect.blockchain.source.subscription.addresses"
  private[config] val ADDRESS_SUBSCRIPTION_DOC = "Comma separated list of addresses to receive transactions updates for"

  val KAFKA_TOPIC = "connect.blockchain.source.kafka.topic"
  val KAFKA_TOPIC_DOC = "Specifies the kafka topic to sent the records to."

  val PROGRESS_COUNTER_ENABLED = "connect.progress.enabled"
  val PROGRESS_COUNTER_ENABLED_DOC = "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"

}
