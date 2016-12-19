package com.datamountaineer.streamreactor.connect.redis.sink.config

import com.datamountaineer.streamreactor.connect.redis.sink.support.RedisMockSupport
import com.datamountaineer.streamreactor.connect.rowkeys.StringStructFieldsStringKeyBuilder
import org.scalatest.{Matchers, WordSpec}
import scala.collection.JavaConverters._

/**
  * Using `SELECT .. FROM .. PK .. STOREAS SS` we can promote the value of one field to a Redis Sorted Set (SS)
  *
  * The `score` can:
  * 1. Be explicitly defined STOREAS SS (score=ts)
  * 2. If not, try to use the field `timestamp` (if it exists)
  * 3. If not does not exist use current time as the timestamp <system.now>
  */
class ConfigMultipleSortedSetsTest extends WordSpec with Matchers with RedisMockSupport {

  // A Sorted Set will be used for every sensorID
  val KCQL1 = "SELECT temperature, humidity FROM sensorsTopic PK sensorID STOREAS SS"
  KCQL1 in {
    val config = getMockRedisSinkConfig(password = true, KCQL = Option(KCQL1))
    val settings = RedisSinkSettings(config)
    val route = settings.allKCQLSettings.head.kcqlConfig
    val fields = route.getFieldAlias.asScala.toList

    settings.allKCQLSettings.head.builder.isInstanceOf[StringStructFieldsStringKeyBuilder] shouldBe true

    route.getStoredAs shouldBe "SS"
    route.isIncludeAllFields shouldBe false
    fields.length == 2
    route.getSource shouldBe "sensorsTopic"
    route.getTarget shouldBe null
  }

  // If you want your Sorted Set to be prefixed use the INSERT
  val KCQL2 = "INSERT INTO SENSOR- SELECT temperature, humidity FROM sensorsTopic PK sensorID STOREAS SS"
  // This will store the SS as   Key=SENSOR-<sensorID>
  KCQL2 in {
    val config = getMockRedisSinkConfig(password = true, KCQL = Option(KCQL2))
    val settings = RedisSinkSettings(config)
    val route = settings.allKCQLSettings.head.kcqlConfig
    val fields = route.getFieldAlias.asScala.toList

    route.getStoredAs shouldBe "SS"
    route.isIncludeAllFields shouldBe false
    route.getTarget shouldBe "SENSOR-"
    fields.length == 2
    route.getSource shouldBe "sensorsTopic"
    route.getPrimaryKeys.next shouldBe "sensorID"
    route.getStoredAs shouldBe "SS"
  }

  // Define which field to use to `score` the entry in the Set
  val KCQL3 = "SELECT * FROM sensorsTopic PK sensorID STOREAS SS (score=ts)"
  KCQL3 in {
    val config = getMockRedisSinkConfig(password = true, KCQL = Option(KCQL3))
    val settings = RedisSinkSettings(config)
    val route = settings.allKCQLSettings.head.kcqlConfig
    val fields = route.getFieldAlias.asScala.toList

    route.isIncludeAllFields shouldBe true
    route.getTarget shouldBe null
    route.getSource shouldBe "sensorsTopic"
    route.getPrimaryKeys.next shouldBe "sensorID"
    route.getStoredAs shouldBe "SS"
    route.getStoredAsParameters.asScala shouldBe Map("score" -> "ts")
  }

  // Define the Date | DateTime format to use to parse the `score` field (store millis in redis)
  val KCQL4 = "SELECT temperature, humidity FROM sensorsTopic PK sensorID STOREAS SS (score=ts, to=yyyyMMddHHmmss)"
  KCQL4 in {
    val config = getMockRedisSinkConfig(password = true, KCQL = Option(KCQL4))
    val settings = RedisSinkSettings(config)
    val route = settings.allKCQLSettings.head.kcqlConfig
    val fields = route.getFieldAlias.asScala.toList

    route.isIncludeAllFields shouldBe false
    fields.length == 1
    route.getTarget shouldBe null
    fields.length == 2
    route.getSource shouldBe "sensorsTopic"
    route.getPrimaryKeys.next shouldBe "sensorID"
    route.getStoredAs shouldBe "SS"
  }
}

// TODO: Introduce WHERE capability
