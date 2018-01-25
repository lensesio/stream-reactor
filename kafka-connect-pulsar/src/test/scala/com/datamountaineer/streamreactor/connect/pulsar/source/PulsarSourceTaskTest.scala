package com.datamountaineer.streamreactor.connect.pulsar.source

import com.datamountaineer.streamreactor.connect.converters.source.{Converter, JsonSimpleConverter}
import com.datamountaineer.streamreactor.connect.pulsar.config.{PulsarConfigConstants, PulsarSourceConfig, PulsarSourceSettings}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 24/01/2018. 
  * stream-reactor
  */
class PulsarSourceTaskTest extends WordSpec with Matchers {

  val pulsarTopic = "persistent://landoop/standalone/connect/kafka-topic"

  "should start create a task with a converter" in {
    val kcql = s"INSERT INTO kafka_topic SELECT * FROM $pulsarTopic BATCH = 10 WITHCONVERTER=`com.datamountaineer.streamreactor.connect.converters.source.JsonSimpleConverter` WITHSUBSCRIPTION = SHARED"

    val props =  Map(
      PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG -> s"$kcql",
      PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
      PulsarConfigConstants.POLLING_TIMEOUT_CONFIG -> "500"
    ).asJava

    val config = PulsarSourceConfig(props)
    val settings = PulsarSourceSettings(config, 1)

    val task = new PulsarSourceTask()
    val converters: Map[String, Converter] = task.buildConvertersMap(props, settings)
    converters.size shouldBe 1
    converters.head._2.getClass shouldBe classOf[JsonSimpleConverter]
  }

}
