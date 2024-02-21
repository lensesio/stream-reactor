/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.redis.sink

import io.lenses.streamreactor.connect.redis.sink.config.RedisConfig
import io.lenses.streamreactor.connect.redis.sink.config.RedisConfigConstants
import io.lenses.streamreactor.connect.redis.sink.config.RedisSinkSettings
import io.lenses.streamreactor.connect.redis.sink.support.RedisMockSupport
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RedisSinkTaskTest extends AnyWordSpec with Matchers with RedisMockSupport {

  "work with Cache" -> {
    val KCQL = s"INSERT INTO cache SELECT price from yahoo-fx PK symbol;"

    val props = Map(
      RedisConfigConstants.REDIS_HOST  -> "localhost",
      RedisConfigConstants.REDIS_PORT  -> "0000",
      RedisConfigConstants.KCQL_CONFIG -> KCQL,
    )

    val config   = RedisConfig(props)
    val settings = RedisSinkSettings(config)

    val task = new RedisSinkTask
    task.filterModeCache(settings).kcqlSettings shouldBe settings.kcqlSettings

    task.filterModeInsertSS(settings).kcqlSettings.isEmpty shouldBe true
    task.filterModePKSS(settings).kcqlSettings.isEmpty shouldBe true
  }

  "work with SortedSet" -> {
    val KCQL = s"INSERT INTO topic-1 SELECT * FROM topic1 STOREAS SortedSet(score=ts);"

    val props = Map(
      RedisConfigConstants.REDIS_HOST  -> "localhost",
      RedisConfigConstants.REDIS_PORT  -> "0000",
      RedisConfigConstants.KCQL_CONFIG -> KCQL,
    )

    val config   = RedisConfig(props)
    val settings = RedisSinkSettings(config)

    val task = new RedisSinkTask
    task.filterModeInsertSS(settings).kcqlSettings shouldBe settings.kcqlSettings

    task.filterModeCache(settings).kcqlSettings.isEmpty shouldBe true
    task.filterModePKSS(settings).kcqlSettings.isEmpty shouldBe true
  }

  "work with Multiple SortedSets" -> {
    val KCQL = s"SELECT temperature, humidity FROM sensorsTopic PK sensorID STOREAS SortedSet(score=timestamp);"

    val props = Map(
      RedisConfigConstants.REDIS_HOST  -> "localhost",
      RedisConfigConstants.REDIS_PORT  -> "0000",
      RedisConfigConstants.KCQL_CONFIG -> KCQL,
    )

    val config   = RedisConfig(props)
    val settings = RedisSinkSettings(config)

    val task = new RedisSinkTask
    task.filterModePKSS(settings).kcqlSettings shouldBe settings.kcqlSettings

    task.filterModeCache(settings).kcqlSettings.isEmpty shouldBe true
    task.filterModeInsertSS(settings).kcqlSettings.isEmpty shouldBe true
  }

  "work with Multiple Modes" -> {
    val KCQL =
      s"SELECT temperature, humidity FROM sensorsTopic PK sensorID STOREAS SortedSet(score=timestamp);" +
        s"SELECT temperature, humidity FROM sensorsTopic2 PK sensorID STOREAS SortedSet(score=timestamp);" +
        s"INSERT INTO cache1 SELECT price from yahoo-fx PK symbol;" +
        s"INSERT INTO cache2 SELECT price from googl-fx PK symbol;" +
        s"INSERT INTO cache3 SELECT price from appl-fx PK symbol;" +
        s"INSERT INTO topic-1 SELECT * FROM topic1 STOREAS SortedSet(score=ts);" +
        s"INSERT INTO topic-2 SELECT * FROM topic2 STOREAS SortedSet(score=ts);"

    val props = Map(
      RedisConfigConstants.REDIS_HOST  -> "localhost",
      RedisConfigConstants.REDIS_PORT  -> "0000",
      RedisConfigConstants.KCQL_CONFIG -> KCQL,
    )

    val config   = RedisConfig(props)
    val settings = RedisSinkSettings(config)

    val task = new RedisSinkTask

    //Verify filtered cacheSettings
    val cacheSettings = task.filterModeCache(settings).kcqlSettings
    cacheSettings.size shouldBe 3
    cacheSettings.exists(_.kcqlConfig.getSource == "yahoo-fx") shouldBe true
    cacheSettings.exists(_.kcqlConfig.getSource == "googl-fx") shouldBe true
    cacheSettings.exists(_.kcqlConfig.getSource == "appl-fx") shouldBe true

    //Verify filtered Sorted Set settings
    val ssSettings = task.filterModeInsertSS(settings).kcqlSettings
    ssSettings.size shouldBe 2
    ssSettings.exists(_.kcqlConfig.getSource == "topic1") shouldBe true
    ssSettings.exists(_.kcqlConfig.getSource == "topic2") shouldBe true

    //Verify filtered Multiple Sorted Set settings
    val mssSettings = task.filterModePKSS(settings).kcqlSettings
    mssSettings.size shouldBe 2
    mssSettings.exists(_.kcqlConfig.getSource == "sensorsTopic") shouldBe true
    mssSettings.exists(_.kcqlConfig.getSource == "sensorsTopic2") shouldBe true
  }

  "work with streams" -> {
    val KCQL = s"SELECT temperature, humidity FROM sensorsTopic PK sensorID STOREAS STREAM;"

    val props = Map(
      RedisConfigConstants.REDIS_HOST  -> "localhost",
      RedisConfigConstants.REDIS_PORT  -> "0000",
      RedisConfigConstants.KCQL_CONFIG -> KCQL,
    )

    val config   = RedisConfig(props)
    val settings = RedisSinkSettings(config)

    val task = new RedisSinkTask
    task.filterStream(settings).kcqlSettings.isEmpty shouldBe false
  }
}
