package com.datamountaineer.streamreactor.connect.redis.sink.config

import com.datamountaineer.streamreactor.connect.redis.sink.config.RedisSinkConfig._
import com.datamountaineer.streamreactor.connect.rowkeys.{StringGenericRowKeyBuilder, StringStructFieldsStringKeyBuilder}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class RedisSinkSettingsTest extends WordSpec with Matchers with MockitoSugar {

  val TABLE_NAME_RAW = "someTable"
  val QUERY_ALL = s"INSERT INTO $TABLE_NAME_RAW SELECT * FROM $TABLE_NAME_RAW"
  val QUERY_ALL_KEYS = s"INSERT INTO $TABLE_NAME_RAW SELECT * FROM $TABLE_NAME_RAW PK lastName"
  val QUERY_SELECT = s"INSERT INTO $TABLE_NAME_RAW SELECT lastName as surname, firstName FROM $TABLE_NAME_RAW"
  val QUERY_SELECT_KEYS = s"INSERT INTO $TABLE_NAME_RAW SELECT lastName as surname, firstName FROM $TABLE_NAME_RAW " +
    s"PK surname"
  val QUERY_SELECT_KEYS_BAD = s"INSERT INTO $TABLE_NAME_RAW SELECT lastName as surname, firstName FROM $TABLE_NAME_RAW " +
    s"PK IamABadPersonAndIHateYou"

  "raise a configuration exception if the export map is empty" in {
    intercept[IllegalArgumentException] {
      val config = mock[RedisSinkConfig]
      when(config.getString(REDIS_HOST)).thenReturn("localhost")
      when(config.getInt(REDIS_PORT)).thenReturn(8453)
      when(config.getString(REDIS_PASSWORD)).thenReturn("secret")
      when(config.getString(RedisSinkConfig.ERROR_POLICY)).thenReturn("THROW")
      RedisSinkSettings(config, List(""))
    }
  }

  "correctly create a RedisSettings when fields are row keys are provided" in {
    val config = mock[RedisSinkConfig]

    when(config.getString(REDIS_HOST)).thenReturn("localhost")
    when(config.getInt(REDIS_PORT)).thenReturn(8453)
    when(config.getString(REDIS_PASSWORD)).thenReturn("secret")
    when(config.getString(EXPORT_ROUTE_QUERY)).thenReturn(QUERY_ALL_KEYS)
    when(config.getString(RedisSinkConfig.ERROR_POLICY)).thenReturn("THROW")

    val settings = RedisSinkSettings(config, List(TABLE_NAME_RAW))
    val route = settings.routes.head

    settings.rowKeyModeMap.get(TABLE_NAME_RAW).get.isInstanceOf[StringStructFieldsStringKeyBuilder] shouldBe true

    route.isIncludeAllFields shouldBe true
    route.getTarget shouldBe TABLE_NAME_RAW
    route.getSource shouldBe TABLE_NAME_RAW
  }

  "correctly create a RedisSettings when no row fields are provided" in {
    val config = mock[RedisSinkConfig]

    when(config.getString(REDIS_HOST)).thenReturn("localhost")
    when(config.getInt(REDIS_PORT)).thenReturn(8453)
    when(config.getString(REDIS_PASSWORD)).thenReturn("secret")
    when(config.getString(EXPORT_ROUTE_QUERY)).thenReturn(QUERY_ALL)
    when(config.getString(RedisSinkConfig.ERROR_POLICY)).thenReturn("THROW")

    val settings = RedisSinkSettings(config, List(TABLE_NAME_RAW))

    settings.rowKeyModeMap.get(TABLE_NAME_RAW).get.isInstanceOf[StringGenericRowKeyBuilder] shouldBe true
    val route = settings.routes.head

    route.isIncludeAllFields shouldBe true
    route.getSource shouldBe TABLE_NAME_RAW
    route.getTarget shouldBe TABLE_NAME_RAW
  }

  "correctly create a RedisSettings when no row fields are provided and selection" in {
    val config = mock[RedisSinkConfig]

    when(config.getString(REDIS_HOST)).thenReturn("localhost")
    when(config.getInt(REDIS_PORT)).thenReturn(8453)
    when(config.getString(REDIS_PASSWORD)).thenReturn("secret")
    when(config.getString(EXPORT_ROUTE_QUERY)).thenReturn(QUERY_SELECT)
    when(config.getString(RedisSinkConfig.ERROR_POLICY)).thenReturn("THROW")

    val settings = RedisSinkSettings(config, List(TABLE_NAME_RAW))
    val route = settings.routes.head
    val fields = route.getFieldAlias.asScala.toList

    settings.rowKeyModeMap.get(TABLE_NAME_RAW).get.isInstanceOf[StringGenericRowKeyBuilder] shouldBe true

    route.isIncludeAllFields shouldBe false
    route.getSource shouldBe TABLE_NAME_RAW
    route.getTarget shouldBe TABLE_NAME_RAW
    fields.head.getField shouldBe "lastName"
    fields.head.getAlias shouldBe "surname"
    fields.last.getField shouldBe "firstName"
    fields.last.getAlias shouldBe "firstName"
  }

  "correctly create a RedisSettings when row fields are provided and selection" in {
    val config = mock[RedisSinkConfig]

    when(config.getString(REDIS_HOST)).thenReturn("localhost")
    when(config.getInt(REDIS_PORT)).thenReturn(8453)
    when(config.getString(REDIS_PASSWORD)).thenReturn("secret")
    when(config.getString(EXPORT_ROUTE_QUERY)).thenReturn(QUERY_SELECT_KEYS)
    when(config.getString(RedisSinkConfig.ERROR_POLICY)).thenReturn("THROW")

    val settings = RedisSinkSettings(config, List(TABLE_NAME_RAW))
    val route = settings.routes.head
    val fields = route.getFieldAlias.asScala.toList

    settings.rowKeyModeMap.get(TABLE_NAME_RAW).get.isInstanceOf[StringStructFieldsStringKeyBuilder] shouldBe true

    route.isIncludeAllFields shouldBe false
    route.getSource shouldBe TABLE_NAME_RAW
    route.getTarget shouldBe TABLE_NAME_RAW
    fields.head.getField shouldBe "lastName"
    fields.head.getAlias shouldBe "surname"
    fields.last.getField shouldBe "firstName"
    fields.last.getAlias shouldBe "firstName"
  }

  "raise an exception when the row key builder is set to FIELDS but pks not in query map" in {
    intercept[java.lang.IllegalArgumentException] {
      val config = mock[RedisSinkConfig]
      when(config.getString(REDIS_HOST)).thenReturn("localhost")
      when(config.getInt(REDIS_PORT)).thenReturn(8453)
      when(config.getString(REDIS_PASSWORD)).thenReturn("secret")
      when(config.getString(EXPORT_ROUTE_QUERY)).thenReturn(QUERY_SELECT_KEYS_BAD) //set keys in select
      when(config.getString(RedisSinkConfig.ERROR_POLICY)).thenReturn("THROW")
      RedisSinkSettings(config, List(TABLE_NAME_RAW))
    }
  }
}
