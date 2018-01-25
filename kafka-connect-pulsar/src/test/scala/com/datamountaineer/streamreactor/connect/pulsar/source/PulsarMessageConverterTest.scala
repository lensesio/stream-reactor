package com.datamountaineer.streamreactor.connect.pulsar.source

import java.util

import com.datamountaineer.streamreactor.connect.pulsar.config.{PulsarConfigConstants, PulsarSourceConfig, PulsarSourceSettings}
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import org.apache.kafka.connect.source.SourceRecord
import org.apache.pulsar.client.api.MessageBuilder
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 24/01/2018. 
  * stream-reactor
  */
class PulsarMessageConverterTest extends WordSpec with Matchers with ConverterUtil {

  val pulsarTopic = "persistent://landoop/standalone/connect/kafka-topic"
  val jsonMessage = "{\"int8\":12,\"int16\":12,\"int32\":12,\"int64\":12,\"float32\":12.2,\"float64\":12.2,\"boolean\":true,\"string\":\"foo\"}"

  "should convert messages" in {
    val props =  Map(
      PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG -> s"INSERT INTO kafka_topic SELECT * FROM $pulsarTopic BATCH = 10",
      PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
      PulsarConfigConstants.POLLING_TIMEOUT_CONFIG -> "500"
    ).asJava

    val config = PulsarSourceConfig(props)
    val settings = PulsarSourceSettings(config, 1)

    // test part of the task here aswell
    val task = new PulsarSourceTask()
    val convertersMap = task.buildConvertersMap(props, settings)

    val converter = PulsarMessageConverter(convertersMap, settings.kcql, false, 100, 100)

    val message = MessageBuilder
      .create
      .setContent(jsonMessage.getBytes)
      .setKey("landoop")
      .setSequenceId(1)
      .build()


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
