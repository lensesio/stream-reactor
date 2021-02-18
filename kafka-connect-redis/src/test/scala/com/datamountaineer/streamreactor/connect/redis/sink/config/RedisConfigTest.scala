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

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.JavaConverters._

class RedisConfigTest extends AnyWordSpec with Matchers {

  "RedisSinkConfig" should {

    "work without a <password>" in {
      RedisConfig.config.parse(propsWithoutPass.asJava)
    }
    "work with <password>" in {
      RedisConfig.config.parse((propsWithoutPass + (RedisConfigConstants.REDIS_PASSWORD -> "pass")).asJava)
    }

    "use custom delimiter for primary key" in {
      RedisConfig.config.parse((propsWithoutPass + (RedisConfigConstants.REDIS_PK_DELIMITER -> "-")).asJava)
    }

  }

  val propsWithoutPass = Map(RedisConfigConstants.REDIS_HOST -> "localhost",
    RedisConfigConstants.REDIS_PORT -> 8453,
    RedisConfigConstants.KCQL_CONFIG -> "SELECT * FROM topicA",
    RedisConfigConstants.ERROR_POLICY -> "THROW")

}
