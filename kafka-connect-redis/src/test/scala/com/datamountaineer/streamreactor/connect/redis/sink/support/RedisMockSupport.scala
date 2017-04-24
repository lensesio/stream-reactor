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

import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisSinkConfig, RedisSinkConfigConstants}
import org.apache.kafka.common.config.types.Password
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

trait RedisMockSupport extends MockitoSugar {

  def getMockRedisSinkConfig(password: Boolean, KCQL: Option[String]) = {
    val config = mock[RedisSinkConfig]
    when(config.getString(RedisSinkConfigConstants.REDIS_HOST)).thenReturn("localhost")
    when(config.getInt(RedisSinkConfigConstants.REDIS_PORT)).thenReturn(8453)
    when(config.getString(RedisSinkConfigConstants.ERROR_POLICY)).thenReturn("THROW")
    if (password) {
      when(config.getPassword(RedisSinkConfigConstants.REDIS_PASSWORD)).thenReturn(new Password("secret"))
      when(config.getString(RedisSinkConfigConstants.REDIS_PASSWORD)).thenReturn("secret")
    }
    if (KCQL.isDefined)
      when(config.getString(RedisSinkConfigConstants.KCQL_CONFIG)).thenReturn(KCQL.get)
    config
  }

}
