package com.datamountaineer.streamreactor.connect.redis.sink.config

import com.datamountaineer.streamreactor.connect.rowkeys.{StringGenericRowKeyBuilder, StringStructFieldsStringKeyBuilder}
import com.datamountaineer.streamreactor.connect.redis.sink.support.RedisMockSupport
import org.scalatest.{Matchers, WordSpec}
import scala.collection.JavaConverters._

class RedisSinkSettingsTest extends WordSpec with Matchers with RedisMockSupport {

  "throw [config exception] if NO KCQL is provided" in {
    intercept[IllegalArgumentException] {
      RedisSinkSettings(getMockRedisSinkConfig(password = true, KCQL = None))
    }
  }

  "work without a <password>" in {
    val KCQL = "SELECT * FROM topicA PK lastName"
    val settings = RedisSinkSettings(getMockRedisSinkConfig(password = false, KCQL = Option(KCQL)))
    settings.connectionInfo.password shouldBe None
  }

  "work with KCQL : SELECT * FROM topicA" in {
    val QUERY_ALL = "SELECT * FROM topicA"
    val config = getMockRedisSinkConfig(password = true, KCQL = Option(QUERY_ALL))
    val settings = RedisSinkSettings(config)

    settings.connectionInfo.password shouldBe Some("secret")
    settings.kcqlSettings.head.builder.isInstanceOf[StringGenericRowKeyBuilder] shouldBe true
    val route = settings.kcqlSettings.head.kcqlConfig

    route.isIncludeAllFields shouldBe true
    route.getSource shouldBe "topicA"
    route.getTarget shouldBe null
  }

  "work with KCQL : SELECT * FROM topicA PK lastName" in {
    val KCQL = s"INSERT INTO xx SELECT * FROM topicA PK lastName"
    val config = getMockRedisSinkConfig(password = true, KCQL = Option(KCQL))
    val settings = RedisSinkSettings(config)
    val route = settings.kcqlSettings.head.kcqlConfig

    settings.kcqlSettings.head.builder.isInstanceOf[StringStructFieldsStringKeyBuilder] shouldBe true

    route.isIncludeAllFields shouldBe true
    route.getTarget shouldBe "xx"
    route.getSource shouldBe "topicA"
  }

  "work with KCQL : SELECT firstName, lastName as surname FROM topicA" in {
    val KCQL = s"INSERT INTO xx SELECT firstName, lastName as surname FROM topicA"
    val config = getMockRedisSinkConfig(password = true, KCQL = Option(KCQL))
    val settings = RedisSinkSettings(config).kcqlSettings.head
    val route = settings.kcqlConfig
    val fields = route.getFieldAlias.asScala.toList

    settings.builder.isInstanceOf[StringGenericRowKeyBuilder] shouldBe true

    route.isIncludeAllFields shouldBe false
    route.getSource shouldBe "topicA"
    route.getTarget shouldBe "xx"
    fields.head.getField shouldBe "firstName"
    fields.head.getAlias shouldBe "firstName"
    fields.last.getField shouldBe "lastName"
    fields.last.getAlias shouldBe "surname"
  }

  "work with KCQL : SELECT firstName, lastName as surname FROM topicA PK surname" in {
    val KCQL = s"INSERT INTO xx SELECT firstName, lastName as surname FROM topicA PK surname"
    val config = getMockRedisSinkConfig(password = true, KCQL = Option(KCQL))
    val settings = RedisSinkSettings(config)
    val route = settings.kcqlSettings.head.kcqlConfig
    val fields = route.getFieldAlias.asScala.toList

    settings.kcqlSettings.head.builder.isInstanceOf[StringStructFieldsStringKeyBuilder] shouldBe true

    route.isIncludeAllFields shouldBe false
    route.getSource shouldBe "topicA"
    route.getTarget shouldBe "xx"
    fields.head.getField shouldBe "firstName"
    fields.head.getAlias shouldBe "firstName"
    fields.last.getField shouldBe "lastName"
    fields.last.getAlias shouldBe "surname"
  }

}
