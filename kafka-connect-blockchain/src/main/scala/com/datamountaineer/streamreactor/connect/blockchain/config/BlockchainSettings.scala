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

import org.apache.kafka.common.config.AbstractConfig

import scala.concurrent.duration.{FiniteDuration, _}

case class BlockchainSettings(url: String,
                              kafkaTopic: String,
                              addresses: Set[String],
                              openConnectionTimeout: FiniteDuration = 10.seconds,
                              keepAlive:FiniteDuration = 25.seconds,
                              bufferSize:Int = 150000)

object BlockchainSettings {
  def apply(config: AbstractConfig): BlockchainSettings = {
    val url = config.getString(BlockchainConfigConstants.CONNECTION_URL)
    require(url != null && !url.trim.isEmpty, s"No ${BlockchainConfigConstants.CONNECTION_URL} provided!")

    val addresses = Option(config.getString(BlockchainConfigConstants.ADDRESS_SUBSCRIPTION)).map(_.split(",").map(_.trim).toSet).getOrElse(Set.empty)
    val kafkaTopic = config.getString(BlockchainConfigConstants.KAFKA_TOPIC)
    require(kafkaTopic != null && kafkaTopic.trim.nonEmpty, s"No ${BlockchainConfigConstants.KAFKA_TOPIC} provided")

    BlockchainSettings(url, kafkaTopic, addresses)
  }
}



