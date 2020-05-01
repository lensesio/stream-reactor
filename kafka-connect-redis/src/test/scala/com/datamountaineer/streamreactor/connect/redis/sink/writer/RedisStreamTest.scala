package com.datamountaineer.streamreactor.connect.redis.sink.writer

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

import java.util

import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisConfig, RedisConfigConstants, RedisConnectionInfo, RedisSinkSettings}
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import redis.clients.jedis.{Jedis, StreamEntryID}

import scala.collection.JavaConverters._

class RedisStreamTest extends AnyWordSpec with Matchers with BeforeAndAfterAll with MockitoSugar {
//
//  val redisServer = new RedisServer(6379)
//
//  override def beforeAll() = redisServer.start()
//
//  override def afterAll() = redisServer.stop()

  "Redis Stream writer" should {

    "write Kafka records to a Redis Stream" in {

      val TOPIC = "cpuTopic"
      val KCQL = s"INSERT INTO stream1 SELECT * from $TOPIC STOREAS STREAM"
      println("Testing KCQL : " + KCQL)
      val props = Map(
        RedisConfigConstants.REDIS_HOST->"localhost",
        RedisConfigConstants.REDIS_PORT->"6379",
        RedisConfigConstants.KCQL_CONFIG->KCQL,
        RedisConfigConstants.REDIS_PASSWORD -> ""
      ).asJava

      val config = RedisConfig(props)
      val connectionInfo = new RedisConnectionInfo("localhost", 6379, None)
      val settings = RedisSinkSettings(config)
      val writer = new RedisStreams(settings)

      val schema = SchemaBuilder.struct().name("com.example.Cpu")
        .field("type", Schema.STRING_SCHEMA)
        .field("temperature", Schema.FLOAT64_SCHEMA)
        .field("voltage", Schema.FLOAT64_SCHEMA)
        .field("ts", Schema.INT64_SCHEMA).build()

      val struct1 = new Struct(schema).put("type", "Xeon").put("temperature", 60.4).put("voltage", 90.1).put("ts", 1482180657010L)

      val sinkRecord1 = new SinkRecord(TOPIC, 0, null, null, schema, struct1, 1)

      val jedis = mock[Jedis]
      writer.jedis = jedis

      val map = new util.HashMap[String, String]()
      map.put("type", "Xeon")
      map.put("temperature", "60.4")
      map.put("voltage", "90.1")
      map.put("ts", 1482180657010L.toString)

      when(jedis.auth("")).isLenient()
      when(jedis.xadd("stream1", null, map)).thenReturn(mock[StreamEntryID])
      writer.initialize(1, settings.errorPolicy)
      writer.write(Seq(sinkRecord1))
    }
  }
}
