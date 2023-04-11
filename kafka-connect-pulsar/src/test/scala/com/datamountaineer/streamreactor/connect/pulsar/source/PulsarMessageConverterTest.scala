/*
 * Copyright 2017-2023 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datamountaineer.streamreactor.connect.pulsar.source

import com.datamountaineer.streamreactor.common.schemas.ConverterUtil
import com.datamountaineer.streamreactor.connect.pulsar.config.PulsarConfigConstants
import com.datamountaineer.streamreactor.connect.pulsar.config.PulsarSourceConfig
import com.datamountaineer.streamreactor.connect.pulsar.config.PulsarSourceSettings
import org.apache.kafka.connect.source.SourceRecord
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util
import scala.annotation.nowarn
import scala.jdk.CollectionConverters.MapHasAsJava

/**
  * Created by andrew@datamountaineer.com on 24/01/2018.
  * stream-reactor
  */
@nowarn
class PulsarMessageConverterTest extends AnyWordSpec with Matchers with ConverterUtil with MockitoSugar {

  val pulsarTopic = "persistent://landoop/standalone/connect/kafka-topic"
  val jsonMessage =
    "{\"int8\":12,\"int16\":12,\"int32\":12,\"int64\":12,\"float32\":12.2,\"float64\":12.2,\"boolean\":true,\"string\":\"foo\"}"

  "should convert messages" in {
    val props = Map(
      PulsarConfigConstants.HOSTS_CONFIG                   -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG                    -> s"INSERT INTO kafka_topic SELECT * FROM $pulsarTopic BATCH = 10",
      PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
      PulsarConfigConstants.POLLING_TIMEOUT_CONFIG         -> "500",
    ).asJava

    val config   = PulsarSourceConfig(props)
    val settings = PulsarSourceSettings(config, 1)

    // test part of the task here aswell
    val task          = new PulsarSourceTask()
    val convertersMap = task.buildConvertersMap(props, settings)

    val converter = PulsarMessageConverter(convertersMap, settings.kcql, false, 100, 100)

    val message = mock[Message[Array[Byte]]]
    when(message.getKey).thenReturn("landoop")
    when(message.getData).thenReturn(jsonMessage.getBytes)
    when(message.getSequenceId).thenReturn(1)
    when(message.getMessageId).thenReturn(MessageId.latest)

    // pulsar message
    converter.convertMessages(message, pulsarTopic)

    val list = new util.ArrayList[SourceRecord]()
    converter.getRecords(list)
    list.size shouldBe 1
    val record = list.get(0)
    record.key().toString shouldBe "landoop"
    record.value().asInstanceOf[Array[Byte]].map(_.toChar).mkString shouldBe jsonMessage
  }
}
