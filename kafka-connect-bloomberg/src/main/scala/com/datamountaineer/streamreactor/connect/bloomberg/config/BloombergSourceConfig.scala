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

package com.datamountaineer.streamreactor.connect.bloomberg.config

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}


class BloombergSourceConfig(map: java.util.Map[String, String]) extends AbstractConfig(BloombergSourceConfig.config, map)

object BloombergSourceConfig {


  lazy val config: ConfigDef = new ConfigDef()
    .define(BloombergSourceConfigConstants.SERVER_HOST, Type.STRING, Importance.HIGH, BloombergSourceConfigConstants.SERVER_HOST_DOC, "Connection", 1, ConfigDef.Width.MEDIUM, BloombergSourceConfigConstants.SERVER_HOST)
    .define(BloombergSourceConfigConstants.SERVER_PORT, Type.INT, Importance.HIGH, BloombergSourceConfigConstants.SERVER_PORT_DOC, "Connection", 2, ConfigDef.Width.MEDIUM, BloombergSourceConfigConstants.SERVER_PORT)
    .define(BloombergSourceConfigConstants.SERVICE_URI, Type.STRING, Importance.HIGH, BloombergSourceConfigConstants.SERVICE_URI_DOC,  "Connection", 3, ConfigDef.Width.MEDIUM, BloombergSourceConfigConstants.SERVICE_URI)
    .define(BloombergSourceConfigConstants.SUBSCRIPTIONS, Type.STRING, Importance.HIGH, BloombergSourceConfigConstants.SUBSCRIPTION_DOC,  "Subscription", 1, ConfigDef.Width.MEDIUM, BloombergSourceConfigConstants.SUBSCRIPTIONS)
    .define(BloombergSourceConfigConstants.AUTHENTICATION_MODE, Type.STRING, Importance.LOW, BloombergSourceConfigConstants.AUTHENTICATION_MODE_DOC, "Connection", 4, ConfigDef.Width.MEDIUM, BloombergSourceConfigConstants.AUTHENTICATION_MODE)
    .define(BloombergSourceConfigConstants.KAFKA_TOPIC, Type.STRING, Importance.HIGH, BloombergSourceConfigConstants.KAFKA_TOPIC_DOC, "Subscription", 1, ConfigDef.Width.MEDIUM, BloombergSourceConfigConstants.KAFKA_TOPIC)
    .define(BloombergSourceConfigConstants.BUFFER_SIZE, Type.INT, Importance.MEDIUM, BloombergSourceConfigConstants.BUFFER_SIZE_DOC, "Connection", 4, ConfigDef.Width.SHORT, BloombergSourceConfigConstants.BUFFER_SIZE)
    .define(BloombergSourceConfigConstants.PAYLOAD_TYPE, Type.STRING, Importance.MEDIUM, BloombergSourceConfigConstants.PAYLOAD_TYPE_DOC, "Subscription", 2, ConfigDef.Width.SHORT, BloombergSourceConfigConstants.PAYLOAD_TYPE)
}
