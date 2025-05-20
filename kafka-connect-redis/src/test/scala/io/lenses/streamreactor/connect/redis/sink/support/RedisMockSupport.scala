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
package io.lenses.streamreactor.connect.redis.sink.support

import io.lenses.streamreactor.connect.redis.sink.config.RedisConfig
import io.lenses.streamreactor.connect.redis.sink.config.RedisConfigConstants
import org.mockito.MockitoSugar

trait RedisMockSupport extends MockitoSugar {

  def getRedisSinkConfig(password: Boolean, KCQL: Option[String], pkDelimiter: Option[String] = None): RedisConfig = {

    val baseProps = scala.collection.mutable.Map[String, String](
      RedisConfigConstants.REDIS_HOST -> "localhost",
      RedisConfigConstants.REDIS_PORT -> "8453",
    )

    if (password) {
      baseProps += RedisConfigConstants.REDIS_PASSWORD -> "secret"
    }

    KCQL.foreach { kcql =>
      baseProps += RedisConfigConstants.KCQL_CONFIG -> kcql
    }

    pkDelimiter.foreach { delimiter =>
      baseProps += RedisConfigConstants.REDIS_PK_DELIMITER -> delimiter
    }

    RedisConfig(baseProps.toMap)
  }

}
