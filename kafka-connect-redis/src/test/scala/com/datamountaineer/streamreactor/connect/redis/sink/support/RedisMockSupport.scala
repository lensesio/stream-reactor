package com.datamountaineer.streamreactor.connect.redis.sink.support

import com.datamountaineer.streamreactor.connect.redis.sink.config.RedisSinkConfig
import com.datamountaineer.streamreactor.connect.redis.sink.config.RedisSinkConfig._
import org.apache.kafka.common.config.types.Password
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

trait RedisMockSupport extends MockitoSugar {

  def getMockRedisSinkConfig(password: Boolean, KCQL: Option[String]) = {
    val config = mock[RedisSinkConfig]
    when(config.getString(REDIS_HOST)).thenReturn("localhost")
    when(config.getInt(REDIS_PORT)).thenReturn(8453)
    when(config.getString(RedisSinkConfig.ERROR_POLICY)).thenReturn("THROW")
    if (password) {
      when(config.getPassword(REDIS_PASSWORD)).thenReturn(new Password("secret"))
      when(config.getString(REDIS_PASSWORD)).thenReturn("secret")
    }
    if (KCQL.isDefined)
      when(config.getString(KCQL_CONFIG)).thenReturn(KCQL.get)
    config
  }

}
