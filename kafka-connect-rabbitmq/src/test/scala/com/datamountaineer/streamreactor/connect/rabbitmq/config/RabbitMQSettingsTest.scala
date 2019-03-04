package com.datamountaineer.streamreactor.connect.rabbitmq.config

import com.datamountaineer.streamreactor.connect.rabbitmq.TestBase
import com.datamountaineer.streamreactor.connect.converters.source._
import org.apache.kafka.common.config.ConfigException
import org.scalatest.{Matchers, WordSpec}
import scala.collection.JavaConverters._

class RabbitMQSettingsTest extends WordSpec with Matchers with TestBase {
    "RabbitMQSettings" should {
        "parse props correctly when creating a new instance with a single KCQL" in {
            val props = getProps1KCQLBase()
            val settings = RabbitMQSettings(props)

            settings.host shouldBe props.get(RabbitMQConfigConstants.HOST_CONFIG)
            settings.username shouldBe props.get(RabbitMQConfigConstants.USER_CONFIG)
            settings.password shouldBe props.get(RabbitMQConfigConstants.PASSWORD_CONFIG)
            settings.port shouldBe props.get(RabbitMQConfigConstants.PORT_CONFIG).toInt
            settings.virtualHost shouldBe props.get(RabbitMQConfigConstants.VIRTUAL_HOST_CONFIG)
            settings.useTls shouldBe props.get(RabbitMQConfigConstants.USE_TLS_CONFIG).toBoolean
            settings.pollingTimeout shouldBe props.get(RabbitMQConfigConstants.POLLING_TIMEOUT_CONFIG).toInt
            settings.sourcesToConvertersMap.size shouldBe 1
            settings.sourcesToConvertersMap.foreach(e => e._2 shouldBe a [BytesConverter])
            settings.kcql.size shouldBe 1
            settings.kcql.toList(0).getSource shouldBe SOURCES(0)
            settings.kcql.toList(0).getTarget shouldBe TARGETS(0)
        }

        "parse props correctly when creating a new instance with multiple KCQLs" in {
            val props = getProps4KCQLBase()
            val settings = RabbitMQSettings(props)

            settings.host shouldBe props.get(RabbitMQConfigConstants.HOST_CONFIG)
            settings.username shouldBe props.get(RabbitMQConfigConstants.USER_CONFIG)
            settings.password shouldBe props.get(RabbitMQConfigConstants.PASSWORD_CONFIG)
            settings.port shouldBe props.get(RabbitMQConfigConstants.PORT_CONFIG).toInt
            settings.virtualHost shouldBe props.get(RabbitMQConfigConstants.VIRTUAL_HOST_CONFIG)
            settings.useTls shouldBe props.get(RabbitMQConfigConstants.USE_TLS_CONFIG).toBoolean
            settings.pollingTimeout shouldBe props.get(RabbitMQConfigConstants.POLLING_TIMEOUT_CONFIG).toInt
            settings.sourcesToConvertersMap.size shouldBe 4
            settings.sourcesToConvertersMap.foreach(e => e._2 shouldBe a [BytesConverter])
            settings.kcql.size shouldBe 4
            settings.kcql.map(_.getSource).toList should contain theSameElementsInOrderAs SOURCES
            settings.kcql.map(_.getTarget).toList should contain theSameElementsInOrderAs TARGETS
        }

        "initialize the correct converter with respect to WITHCONVERTER parameter in KCQL" in {
            val props = getProps4KCQLsWithAllConverters()
            val settings = RabbitMQSettings(props)

            settings.sourcesToConvertersMap.getOrElse(SOURCES(0),fail) shouldBe a [BytesConverter]
            settings.sourcesToConvertersMap.getOrElse(SOURCES(1),fail) shouldBe a [JsonSimpleConverter]
            settings.sourcesToConvertersMap.getOrElse(SOURCES(2),fail) shouldBe a [JsonConverterWithSchemaEvolution]
            settings.sourcesToConvertersMap.getOrElse(SOURCES(3),fail) shouldBe an [AvroConverter]
        }

        "initialize the correct parameters from the ones provided in KCQL" in {
            val props = getProps4KCQLsWithAllParameters()
            val settings = RabbitMQSettings(props)

            settings.sourcesToConvertersMap.getOrElse(SOURCES(0),fail) shouldBe a [BytesConverter]
            settings.sourcesToConvertersMap.getOrElse(SOURCES(1),fail) shouldBe a [JsonSimpleConverter]
            settings.sourcesToConvertersMap.getOrElse(SOURCES(2),fail) shouldBe a [JsonConverterWithSchemaEvolution]
            settings.sourcesToConvertersMap.getOrElse(SOURCES(3),fail) shouldBe an [AvroConverter]
            for (i <- 0 to settings.kcql.size-1) {
                settings.kcql.toList(i).getTags.get(0).getKey shouldBe ROUTING_KEYS(i)
            }
            for (i <- 0 to settings.kcql.size-1) {
                settings.kcql.toList(i).getWithType shouldBe EXCHANGE_TYPE(i)
            }
        }

        "throw a ConfigException if HOST is not provided" in {
            val props = getProps1KCQLBaseNoHost()
            a [ConfigException] should be thrownBy RabbitMQSettings(props)
        }

        "throw a ConfigException if KCQL is not provided" in {
            val props = getPropsNoKCQL()
            a [ConfigException] should be thrownBy RabbitMQSettings(props)
        }

        "throw a ConfigException if a converter class provided cannot be found" in {
            val props = getProps1KCQLNonExistingConverterClass()
            a [ConfigException] should be thrownBy RabbitMQSettings(props)
        }

        "throw a ConfigException if a converter class provided is found but not inheriting from " +
            "com.datamountaineer.streamreactor.connect.converters.source.Converter" in {
            val props = getProps1KCQLProvidedClassNotAConverterClass()
            a [ConfigException] should be thrownBy RabbitMQSettings(props)
        }

        "throw a ConfigException if PORT_CONFIG is not between [1,65535]" in {
            var props1 = getProps1KCQLBase(port = "100450")
            a [ConfigException] should be thrownBy RabbitMQSettings(props1)

            val props2 = getProps1KCQLBase(port = "-5672")
            a [ConfigException] should be thrownBy RabbitMQSettings(props2)
        }

        "throw a ConfigException if POLLING_TIMEOUT_CONFIG <= 0" in {
            val props = getProps1KCQLBase(pollingTimeout = "-3000")
            a [ConfigException] should be thrownBy RabbitMQSettings(props)
        }

    }
}
