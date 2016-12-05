package com.datamountaineer.streamreactor.connect.redis.sink.config

import com.datamountaineer.streamreactor.connect.redis.sink.support.RedisMockSupport
import org.scalatest.{Matchers, WordSpec}
import scala.collection.JavaConverters._

/**
  * Using INSERT we can store data from one Kafka topic into one Redis Sorted Set (SS)
  *
  * The `score` can:
  * 1. Be explicitly defined STOREAS SS (score=ts)
  * 2. If not, try to use the field `timestamp` (if it exists)
  * 3. If not does not exist use current time as the timestamp <system.now>
  */
class Redis_INSERT_SS_Spec extends WordSpec with Matchers with RedisMockSupport {

  // Insert into a Single Sorted Set
  val KCQL1 = "INSERT INTO cpu_stats SELECT * from cpuTopic STOREAS SS"
  KCQL1 in {
    val config = getMockRedisSinkConfig(password = true, KCQL = Option(KCQL1))
    val settings = RedisSinkSettings(config)
    val route = settings.allKCQLSettings.head.kcqlConfig
    val fields = route.getFieldAlias.asScala.toList

    //settings.allKCQLSettings.head.builder.isInstanceOf[StringStructFieldsStringKeyBuilder] shouldBe true

    route.isIncludeAllFields shouldBe true
    // Store all data on a Redis Sorted Set called <cpu_stats>
    route.getTarget shouldBe "cpu_stats"
    route.getSource shouldBe "cpuTopic"
  }

  // Define which field to use to `score` the entry in the Set
  val KCQL2 = "INSERT INTO cpu_stats_SS SELECT temperature from cpuTopic STOREAS SS (score=ts)"
  KCQL2 in {
    val config = getMockRedisSinkConfig(password = true, KCQL = Option(KCQL2))
    val settings = RedisSinkSettings(config)
    val route = settings.allKCQLSettings.head.kcqlConfig
    val fields = route.getFieldAlias.asScala.toList

    route.isIncludeAllFields shouldBe false
    fields.length shouldBe 1
    route.getTarget shouldBe "cpu_stats_SS"
    route.getSource shouldBe "cpuTopic"

    route.getStoredAs shouldBe "SS"
    route.getStoredAsParameters.asScala shouldBe Map("score" -> "ts")
  }

  // Define the Date | DateTime format to use to parse the `score` field (store millis in redis)
  val KCQL3 = "INSERT INTO cpu_stats_SS SELECT * from cpuTopic STOREAS SS (score = ts , to = YYYYMMDDHHSS )"
  KCQL3 in {
    //(param1 = value1 , param2 = value2,param3=value3)
    val config = getMockRedisSinkConfig(password = true, KCQL = Option(KCQL3))
    val settings = RedisSinkSettings(config)
    val route = settings.allKCQLSettings.head.kcqlConfig
    val fields = route.getFieldAlias.asScala.toList

    route.isIncludeAllFields shouldBe true
    route.getTarget shouldBe "cpu_stats_SS"
    route.getSource shouldBe "cpuTopic"

    route.getStoredAs shouldBe "SS"
    route.getStoredAsParameters.asScala shouldBe Map("score" -> "ts", "to" -> "YYYYMMDDHHSS")
  }

}
