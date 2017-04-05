/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.redis.sink.config

import com.datamountaineer.streamreactor.connect.redis.sink.support.RedisMockSupport
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

/**
  * Using INSERT we can store data from one Kafka topic into one Redis Sorted Set (SS)
  *
  * Requires KCQL syntax: .. INSERT .. STOREAS SortedSet(score=ts)
  *
  * The `score` can:
  * 1. Be explicitly defined STOREAS SortedSet (score=ts)
  * 2. If not, try to use the field `timestamp` (if it exists)
  * 3. If not does not exist use current time as the timestamp <system.now>
  */
class ConfigInsertSortedSetTest extends WordSpec with Matchers with RedisMockSupport {

  // Insert into a Single Sorted Set
  val KCQL1 = "INSERT INTO cpu_stats SELECT * from cpuTopic STOREAS SortedSet"
  KCQL1 in {
    val config = getMockRedisSinkConfig(password = true, KCQL = Option(KCQL1))
    val settings = RedisSinkSettings(config)
    val route = settings.kcqlSettings.head.kcqlConfig
    val fields = route.getFieldAlias.asScala.toList

    route.getStoredAs shouldBe "SortedSet"
    route.isIncludeAllFields shouldBe true
    // Store all data on a Redis Sorted Set called <cpu_stats>
    route.getTarget shouldBe "cpu_stats"
    route.getSource shouldBe "cpuTopic"
  }

  // Define which field to use to `score` the entry in the Set
  val KCQL2 = "INSERT INTO cpu_stats_SS SELECT temperature from cpuTopic STOREAS SortedSet (score=ts)"
  KCQL2 in {
    val config = getMockRedisSinkConfig(password = true, KCQL = Option(KCQL2))
    val settings = RedisSinkSettings(config)
    val route = settings.kcqlSettings.head.kcqlConfig
    val fields = route.getFieldAlias.asScala.toList

    route.isIncludeAllFields shouldBe false
    fields.length shouldBe 1
    route.getTarget shouldBe "cpu_stats_SS"
    route.getSource shouldBe "cpuTopic"

    route.getStoredAs shouldBe "SortedSet"
    route.getStoredAsParameters.asScala shouldBe Map("score" -> "ts")
  }

  // Define the Date | DateTime format to use to parse the `score` field (store millis in redis)
  val KCQL3 = "INSERT INTO cpu_stats_SS SELECT * from cpuTopic STOREAS SortedSet (score=ts,to=YYYYMMDDHHSS)"
  KCQL3 in {
    //(param1 = value1 , param2 = value2,param3=value3)
    val config = getMockRedisSinkConfig(password = true, KCQL = Option(KCQL3))
    val settings = RedisSinkSettings(config)
    val route = settings.kcqlSettings.head.kcqlConfig
    val fields = route.getFieldAlias.asScala.toList

    route.isIncludeAllFields shouldBe true
    route.getTarget shouldBe "cpu_stats_SS"
    route.getSource shouldBe "cpuTopic"

    route.getStoredAs shouldBe "SortedSet"
    route.getStoredAsParameters.asScala shouldBe Map("score" -> "ts", "to" -> "YYYYMMDDHHSS")
  }

}
