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

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

object BlockchainConfig {

  val config: ConfigDef = new ConfigDef()
    .define(BlockchainConfigConstants.CONNECTION_URL, Type.STRING, BlockchainConfigConstants.CONNECTION_URL_DEFAULT, Importance.HIGH, BlockchainConfigConstants.CONNECTION_URL_DOC)
    .define(BlockchainConfigConstants.ADDRESS_SUBSCRIPTION, Type.STRING, null, Importance.LOW, BlockchainConfigConstants.ADDRESS_SUBSCRIPTION_DOC)
    .define(BlockchainConfigConstants.KAFKA_TOPIC, Type.STRING, Importance.HIGH, BlockchainConfigConstants.KAFKA_TOPIC_DOC)
    .define(BlockchainConfigConstants.PROGRESS_COUNTER_ENABLED, Type.BOOLEAN, BlockchainConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT, Importance.MEDIUM, BlockchainConfigConstants.PROGRESS_COUNTER_ENABLED_DOC, "Metrics", 1, ConfigDef.Width.MEDIUM, BlockchainConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY)
}

case class BlockchainConfig(props: util.Map[String, String]) extends AbstractConfig(BlockchainConfig.config, props)
