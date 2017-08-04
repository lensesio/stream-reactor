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

package com.datamountaineer.streamreactor.connect.redis.sink.support

import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisConfig, RedisConfigConstants}
import org.apache.kafka.common.config.types.Password
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import scala.collection.JavaConverters._

trait RedisMockSupport extends MockitoSugar {

  def getRedisSinkConfig(password: Boolean, KCQL: Option[String]) = {

    var baseProps = scala.collection.mutable.Map[String, String]()
    baseProps += (RedisConfigConstants.REDIS_HOST->"localhost", RedisConfigConstants.REDIS_PORT->"8453")


    if (password) {
      baseProps += RedisConfigConstants.REDIS_PASSWORD->"secret"
    }

    if (KCQL.isDefined) {
      baseProps += RedisConfigConstants.KCQL_CONFIG->KCQL.get
    }
    RedisConfig(baseProps.asJava)
  }

}
