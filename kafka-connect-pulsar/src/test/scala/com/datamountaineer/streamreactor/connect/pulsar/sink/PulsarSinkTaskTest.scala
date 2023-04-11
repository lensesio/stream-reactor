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
package com.datamountaineer.streamreactor.connect.pulsar.sink

import com.datamountaineer.streamreactor.connect.pulsar.SlowTest

import java.util
import com.datamountaineer.streamreactor.connect.pulsar.config.PulsarConfigConstants
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters.MapHasAsJava

/**
  * Created by andrew@datamountaineer.com on 24/01/2018.
  * stream-reactor
  */
class PulsarSinkTaskTest extends AnyWordSpec with Matchers with MockitoSugar {

  val pulsarTopic = "persistent://landoop/standalone/connect/kafka-topic"

  "should start a Sink" taggedAs SlowTest in {
    val props = Map(
      PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG  -> s"INSERT INTO $pulsarTopic SELECT * FROM kafka_topic BATCH = 10 WITHPARTITIONER = SinglePartition WITHCOMPRESSION = ZLIB WITHDELAY = 1000",
    ).asJava

    val assignment: util.Set[TopicPartition] = new util.HashSet[TopicPartition]
    val partition:  TopicPartition           = new TopicPartition("kafka_topic", 1)
    //Set topic assignments
    assignment.add(partition)
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(assignment)
    when(context.configs()).thenReturn(props)
    val task = new PulsarSinkTask()
    task.initialize(context)
    task.start(props)
  }
}
