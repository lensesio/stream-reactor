/*
 * Copyright 2017-2025 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.redis.sink.config

import io.lenses.streamreactor.connect.redis.sink.rowkeys.StringStructFieldsStringKeyBuilder
import io.lenses.streamreactor.connect.redis.sink.support.RedisMockSupport
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

/**
  * Using `SELECT .. FROM .. PK .. STOREAS SortedSet` we can promote the value of one field to a Redis Sorted Set (SS)
  *
  * The `score` can:
  * 1. Be explicitly defined STOREAS SortedSet (score=ts)
  * 2. If not, try to use the field `timestamp` (if it exists)
  * 3. If not does not exist use current time as the timestamp <system.now>
  */
class ConfigMultipleSortedSetsTest extends AnyWordSpec with Matchers with RedisMockSupport {

  // A Sorted Set will be used for every sensorID
  val KCQL1 = "SELECT temperature, humidity FROM sensorsTopic PK sensorID STOREAS SortedSet TTL = 60"
  KCQL1 in {
    val config   = getRedisSinkConfig(password = true, KCQL = Option(KCQL1))
    val settings = RedisSinkSettings(config)
    val route    = settings.kcqlSettings.head.kcqlConfig

    settings.kcqlSettings.head.builder.isInstanceOf[StringStructFieldsStringKeyBuilder] shouldBe true

    route.getStoredAs shouldBe "SortedSet"
    route.getFields.asScala.exists(_.getName.equals("*")) shouldBe false
    route.getSource shouldBe "sensorsTopic"
    route.getTarget shouldBe null
  }

  // If you want your Sorted Set to be prefixed use the INSERT
  val KCQL2 =
    "INSERT INTO SENSOR- SELECT temperature, humidity FROM sensorsTopic PK sensorID STOREAS SortedSet TTL = 60"
  // This will store the SortedSet as   Key=SENSOR-<sensorID>
  KCQL2 in {
    val config   = getRedisSinkConfig(password = true, KCQL = Option(KCQL2))
    val settings = RedisSinkSettings(config)
    val route    = settings.kcqlSettings.head.kcqlConfig
    val fields   = route.getFields.asScala.toList

    route.getPrimaryKeys.asScala.head.getName shouldBe "sensorID"
    route.getFields.asScala.exists(_.getName.equals("*")) shouldBe false
    route.getSource shouldBe "sensorsTopic"
    route.getStoredAs shouldBe "SortedSet"
    route.getTarget shouldBe "SENSOR-"
    fields.length == 2
  }

  // Define which field to use to `score` the entry in the Set
  val KCQL3 = "SELECT * FROM sensorsTopic PK sensorID STOREAS SortedSet (score=ts)"
  KCQL3 in {
    val config   = getRedisSinkConfig(password = true, KCQL = Option(KCQL3))
    val settings = RedisSinkSettings(config)
    val route    = settings.kcqlSettings.head.kcqlConfig

    route.getStoredAsParameters.asScala shouldBe Map("score" -> "ts")
    route.getPrimaryKeys.asScala.head.getName shouldBe "sensorID"
    route.getFields.asScala.exists(_.getName.equals("*")) shouldBe true
    route.getSource shouldBe "sensorsTopic"
    route.getStoredAs shouldBe "SortedSet"
    route.getTarget shouldBe null
  }

  // Define the Date | DateTime format to use to parse the `score` field (store millis in redis)
  val KCQL4 =
    "SELECT temperature, humidity FROM sensorsTopic PK sensorID STOREAS SortedSet (score=ts, to=yyyyMMddHHmmss)"
  KCQL4 in {
    val config   = getRedisSinkConfig(password = true, KCQL = Option(KCQL4))
    val settings = RedisSinkSettings(config)
    val route    = settings.kcqlSettings.head.kcqlConfig
    val fields   = route.getFields.asScala.toList

    route.getPrimaryKeys.asScala.head.getName shouldBe "sensorID"
    route.getFields.asScala.exists(_.getName.equals("*")) shouldBe false
    route.getSource shouldBe "sensorsTopic"
    route.getStoredAs shouldBe "SortedSet"
    route.getTarget shouldBe null
    fields.length shouldBe 2
  }
}
