/*
 * *
 *   * Copyright 2016 Datamountaineer.
 *   *
 *   * Licensed under the Apache License, Version 2.0 (the "License");
 *   * you may not use this file except in compliance with the License.
 *   * You may obtain a copy of the License at
 *   *
 *   * http://www.apache.org/licenses/LICENSE-2.0
 *   *
 *   * Unless required by applicable law or agreed to in writing, software
 *   * distributed under the License is distributed on an "AS IS" BASIS,
 *   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   * See the License for the specific language governing permissions and
 *   * limitations under the License.
 *   *
 */
package com.datamountaineer.streamreactor.connect.mqtt.source.converters

import java.util.Collections

import com.datamountaineer.streamreactor.connect.mqtt.source.{MqttMsgKey, SourceConstants}
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord

class BytesConverter extends MqttConverter {
  override def convert(kafkaTopic: String, mqttSource: String, messageId: Int, bytes: Array[Byte]): SourceRecord = {
    new SourceRecord(Collections.singletonMap(SourceConstants.PartitionKey, mqttSource),
      null,
      kafkaTopic,
      MqttMsgKey.schema,
      MqttMsgKey.getStruct(mqttSource, messageId),
      Schema.BYTES_SCHEMA,
      bytes)
  }

  override def initialize(map: Map[String, String]): Unit = {}
}
