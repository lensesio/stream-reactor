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

import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConversions._

class RedisSinkConfigTest extends WordSpec with Matchers {

  "RedisSinkConfig" should {

    "work without a <password>" in {
      RedisSinkConfig.config.parse(propsWithoutPass)
    }
    "work with <password>" in {
      RedisSinkConfig.config.parse(propsWithoutPass + (RedisSinkConfigConstants.REDIS_PASSWORD -> "pass"))
    }

  }

  val propsWithoutPass = Map(RedisSinkConfigConstants.REDIS_HOST -> "localhost",
    RedisSinkConfigConstants.REDIS_PORT -> 8453,
    RedisSinkConfigConstants.KCQL_CONFIG -> "SELECT * FROM topicA",
    RedisSinkConfigConstants.ERROR_POLICY -> "THROW")

}
