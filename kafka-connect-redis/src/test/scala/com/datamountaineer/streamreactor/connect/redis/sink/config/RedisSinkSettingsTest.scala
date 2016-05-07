package com.datamountaineer.streamreactor.connect.redis.sink.config

import com.datamountaineer.streamreactor.connect.redis.sink.config.RedisSinkConfig._
import org.apache.kafka.common.config.ConfigException
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

class RedisSinkSettingsTest extends WordSpec with Matchers with MockitoSugar {
  "RedisSinkSettings" should {
    "raise a configuration exception if key mode is not valid" in {
      intercept[ConfigException] {
        val config = mock[RedisSinkConfig]
        when(config.getString(REDIS_HOST)).thenReturn("localhost")
        when(config.getInt(REDIS_PORT)).thenReturn(8453)
        when(config.getString(REDIS_PASSWORD)).thenReturn("secret")
        when(config.getString(ROW_KEY_MODE)).thenReturn("wrong")
        RedisSinkSettings(config)
      }
    }

    "raise a configuration exception if key mode is set to FIELDS but no fields are provided" in {
      intercept[ConfigException] {
        val config = mock[RedisSinkConfig]
        when(config.getString(REDIS_HOST)).thenReturn("localhost")
        when(config.getInt(REDIS_PORT)).thenReturn(8453)
        when(config.getString(REDIS_PASSWORD)).thenReturn("secret")
        when(config.getString(ROW_KEY_MODE)).thenReturn("FIELDS")
        RedisSinkSettings(config)
      }
    }
    "create a instance of RedisSinkSettings with FIELDS key mode and payload fields mappings" in {
      val config = mock[RedisSinkConfig]
      when(config.getString(REDIS_HOST)).thenReturn("localhost")
      when(config.getInt(REDIS_PORT)).thenReturn(8453)
      when(config.getString(REDIS_PASSWORD)).thenReturn("secret")
      when(config.getString(ROW_KEY_MODE)).thenReturn("FIELDS")
      when(config.getString(ROW_KEYS)).thenReturn("field1,field2")
      when(config.getString(FIELDS)).thenReturn("*, field1=someAlias")
      val settings = RedisSinkSettings(config)

      settings.connection.host shouldBe "localhost"
      settings.connection.port shouldBe 8453
      settings.connection.password.get shouldBe "secret"

      settings.key.mode shouldBe "FIELDS"
      settings.key.fields shouldBe Seq("field1", "field2")

      settings.fields.includeAllFields shouldBe true
      settings.fields.fieldsMappings.size shouldBe 1
      settings.fields.fieldsMappings.get("field1").get shouldBe "someAlias"
    }
  }
}
