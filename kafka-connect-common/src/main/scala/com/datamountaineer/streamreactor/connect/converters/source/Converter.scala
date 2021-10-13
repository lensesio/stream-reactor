/*
 *
 *  * Copyright 2020 Lenses.io.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package com.datamountaineer.streamreactor.connect.converters.source

import org.apache.kafka.connect.source.SourceRecord

/**
  * Provides the interface for converting a Connect source payload (JMS, MQTT, etc) to a SourceRecord
  */
trait Converter {
  def initialize(map: Map[String, String]): Unit = {}

  def convert(kafkaTopic: String,
              sourceTopic: String,
              messageId: String,
              bytes: Array[Byte],
              keys: Seq[String] = Seq.empty,
              keyDelimiter: String = ".",
              properties: Map[String, String] = Map.empty): SourceRecord
}

object Converter {
  val TopicKey = "topic"
  val CONNECT_SOURCE_CONVERTER_PREFIX = "connect.source.converter"
}
