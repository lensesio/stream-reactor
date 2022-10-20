package com.datamountaineer.streamreactor.connect.redis.sink.config

import com.datamountaineer.streamreactor.connect.redis.sink.support.RedisMockSupport
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

/**
  * Using `SELECT .. FROM .. PK .. STOREAS GeoAdd` we can insert a record form topic into Redis with GEOADD command
  *
  * The `longitudeField` and `latitudeField` can:
  * 1. Be explicitly defined STOREAS GeoAdd (longitudeField=lng,latitudeField=lat)
  * 2. If not, try to use the field `longitude` and `latitude` (if it exists)
  * 3. If not does not exist record will be skipped
  */

class ConfigGeoAddTest extends AnyWordSpec with Matchers with RedisMockSupport {

  // GEOADD with PK
  val KCQL1 = "SELECT * from addressTopic PK addressId STOREAS GeoAdd"
  KCQL1 in {
    val config   = getRedisSinkConfig(password = true, KCQL = Option(KCQL1))
    val settings = RedisSinkSettings(config)
    val route    = settings.kcqlSettings.head.kcqlConfig

    route.getStoredAs shouldBe "GeoAdd"
    route.getFields.asScala.exists(_.getName.equals("*")) shouldBe true
    route.getPrimaryKeys.asScala.head.getName shouldBe "addressId"
    route.getTarget shouldBe null
    route.getSource shouldBe "addressTopic"
  }

  // GEOADD with PK and prefix
  val KCQL2 = "INSERT INTO address_set SELECT * from addressTopic PK addressId STOREAS GeoAdd"
  KCQL2 in {
    val config   = getRedisSinkConfig(password = true, KCQL = Option(KCQL2))
    val settings = RedisSinkSettings(config)
    val route    = settings.kcqlSettings.head.kcqlConfig

    route.getStoredAs shouldBe "GeoAdd"
    route.getFields.asScala.exists(_.getName.equals("*")) shouldBe true
    route.getPrimaryKeys.asScala.head.getName shouldBe "addressId"
    route.getTarget shouldBe "address_set"
    route.getSource shouldBe "addressTopic"
  }

  // GEOADD with PK, prefix, storedAsParameters
  val KCQL3: String = "INSERT INTO address_set SELECT country from addressTopic PK addressId " +
    "STOREAS GeoAdd (longitudeField=lng, latitudeField=lat)"
  KCQL3 in {
    val config   = getRedisSinkConfig(password = true, KCQL = Option(KCQL3))
    val settings = RedisSinkSettings(config)
    val route    = settings.kcqlSettings.head.kcqlConfig
    val fields   = route.getFields.asScala.toList

    route.getFields.asScala.exists(_.getName.equals("*")) shouldBe false
    fields.length shouldBe 1
    route.getTarget shouldBe "address_set"
    route.getSource shouldBe "addressTopic"

    route.getStoredAs shouldBe "GeoAdd"
    route.getStoredAsParameters.asScala shouldBe Map("longitudeField" -> "lng", "latitudeField" -> "lat")
  }
}
