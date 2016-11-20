package com.datamountaineer.streamreactor.connect.redis.sink.config

import com.datamountaineer.streamreactor.connect.redis.sink.config.RedisSinkConfig._
import com.datamountaineer.streamreactor.connect.rowkeys.{StringGenericRowKeyBuilder, StringStructFieldsStringKeyBuilder}
import org.apache.kafka.common.config.types.Password
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class RedisSinkSettingsTest extends WordSpec with Matchers with MockitoSugar {

  "throw [config exception] if NO KCQL is provided" in {
    intercept[IllegalArgumentException] {
      RedisSinkSettings(getMockRedisSinkConfig(password = true, KCQL = None))
    }
  }

  "work without a <password>" in {
    val KCQL = s"INSERT INTO xx SELECT * FROM topicA PK lastName"
    val settings = RedisSinkSettings(getMockRedisSinkConfig(password = false, KCQL = Option(KCQL)))
    settings.connection.password shouldBe None
  }

  "work with KCQL : SELECT * FROM topicA" in {
    val QUERY_ALL = s"INSERT INTO xx SELECT * FROM topicA"
    val config = getMockRedisSinkConfig(password = true, KCQL = Option(QUERY_ALL))
    val settings = RedisSinkSettings(config)

    settings.connection.password shouldBe Some("secret")
    settings.rowKeyModeMap("topicA").isInstanceOf[StringGenericRowKeyBuilder] shouldBe true
    val route = settings.routes.head

    route.isIncludeAllFields shouldBe true
    route.getSource shouldBe "topicA"
    route.getTarget shouldBe "xx"
  }

  "work with KCQL : SELECT * FROM topicA PK lastName" in {
    val KCQL = s"INSERT INTO xx SELECT * FROM topicA PK lastName"
    val config = getMockRedisSinkConfig(password = true, KCQL = Option(KCQL))
    val settings = RedisSinkSettings(config)
    val route = settings.routes.head

    settings.rowKeyModeMap("topicA").isInstanceOf[StringStructFieldsStringKeyBuilder] shouldBe true

    route.isIncludeAllFields shouldBe true
    route.getTarget shouldBe "xx"
    route.getSource shouldBe "topicA"
  }

  "work with KCQL : SELECT firstName, lastName as surname FROM topicA" in {
    val KCQL = s"INSERT INTO xx SELECT firstName, lastName as surname FROM topicA"
    val config = getMockRedisSinkConfig(password = true, KCQL = Option(KCQL))
    val settings = RedisSinkSettings(config)
    val route = settings.routes.head
    val fields = route.getFieldAlias.asScala.toList

    settings.rowKeyModeMap("topicA").isInstanceOf[StringGenericRowKeyBuilder] shouldBe true

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
    val route = settings.routes.head
    val fields = route.getFieldAlias.asScala.toList

    settings.rowKeyModeMap("topicA").isInstanceOf[StringStructFieldsStringKeyBuilder] shouldBe true

    route.isIncludeAllFields shouldBe false
    route.getSource shouldBe "topicA"
    route.getTarget shouldBe "xx"
    fields.head.getField shouldBe "firstName"
    fields.head.getAlias shouldBe "firstName"
    fields.last.getField shouldBe "lastName"
    fields.last.getAlias shouldBe "surname"
  }

  // throw when PK in KCQL does not exist in the message selection
  "throw KCQL exception : SELECT lastName as surname, firstName FROM topicA PK missingField" in {
    intercept[java.lang.IllegalArgumentException] {
      val KCQL_BAD_PK = s"INSERT INTO xx SELECT lastName as surname, firstName FROM topicA PK missingField"
      RedisSinkSettings(getMockRedisSinkConfig(password = true, KCQL = Option(KCQL_BAD_PK)))
    }
  }

  /** Helper methods **/
  def getMockRedisSinkConfig(password: Boolean, KCQL: Option[String]) = {
    val config = mock[RedisSinkConfig]
    when(config.getString(REDIS_HOST)).thenReturn("localhost")
    when(config.getInt(REDIS_PORT)).thenReturn(8453)
    when(config.getString(RedisSinkConfig.ERROR_POLICY)).thenReturn("THROW")
    if (password) {
      when(config.getPassword(REDIS_PASSWORD)).thenReturn(new Password("secret"))
      when(config.getString(REDIS_PASSWORD)).thenReturn("secret")
    }
    if (KCQL.isDefined)
      when(config.getString(KCQL_CONFIG)).thenReturn(KCQL.get)
    config
  }

}
