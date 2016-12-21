package com.datamountaineer.streamreactor.connect.redis.sink.config

import com.datamountaineer.streamreactor.connect.redis.sink.config.RedisSinkConfig._
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConversions._

class RedisSinkConfigTest extends WordSpec with Matchers {

  "RedisSinkConfig" should {

    "work without a <password>" in {
      RedisSinkConfig.config.parse(propsWithoutPass)
    }
    "work with <password>" in {
      RedisSinkConfig.config.parse(propsWithoutPass + (REDIS_PASSWORD -> "pass"))
    }

  }

  val propsWithoutPass = Map(REDIS_HOST -> "localhost",
    REDIS_PORT -> 8453,
    KCQL_CONFIG -> "SELECT * FROM topicA",
    ERROR_POLICY -> "THROW")

}
