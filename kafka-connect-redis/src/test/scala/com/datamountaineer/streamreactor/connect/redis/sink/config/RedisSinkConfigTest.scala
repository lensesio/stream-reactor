package com.datamountaineer.streamreactor.connect.redis.sink.config

import com.datamountaineer.streamreactor.connect.redis.sink.config.RedisSinkConfig._
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConversions._

class RedisSinkConfigTest extends WordSpec with Matchers {
  "RedisSinkConfig" should {
    "not throw an exception if password is not provided" in {
      val props = Map(REDIS_HOST -> "localhost",
        REDIS_PORT -> 8453,
        EXPORT_ROUTE_QUERY -> "INSERT INTO A SELECT * FROM topicA",
        ERROR_POLICY -> "THROW")
      RedisSinkConfig.config.parse(props)
    }
  }
}
