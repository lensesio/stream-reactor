package com.datamountaineer.streamreactor.connect.pulsar.sink

import java.util

import com.datamountaineer.streamreactor.connect.pulsar.config.PulsarConfigConstants
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.Mockito.when
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.mockito.MockitoSugar

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 24/01/2018. 
  * stream-reactor
  */
class PulsarSinkTaskTest extends WordSpec with Matchers with MockitoSugar {

  val pulsarTopic = "persistent://landoop/standalone/connect/kafka-topic"

  "should start a Sink" in {
    val props = Map(
      PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG -> s"INSERT INTO $pulsarTopic SELECT * FROM kafka_topic BATCH = 10 WITHPARTITIONER = SinglePartition WITHCOMPRESSION = ZLIB WITHDELAY = 1000"
    ).asJava


    val assignment: util.Set[TopicPartition] = new util.HashSet[TopicPartition]
    val partition: TopicPartition = new TopicPartition("kafka_topic", 1)
    //Set topic assignments
    assignment.add(partition)
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(assignment)


    val task = new PulsarSinkTask()
    task.start(props)
  }
}
