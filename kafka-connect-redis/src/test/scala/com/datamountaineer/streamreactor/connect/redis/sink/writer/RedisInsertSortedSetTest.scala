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

package com.datamountaineer.streamreactor.connect.redis.sink.writer

import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisConfig, RedisConfigConstants, RedisConnectionInfo, RedisSinkSettings}
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import redis.clients.jedis.Jedis
import redis.embedded.RedisServer

import scala.collection.JavaConverters._

class RedisInsertSortedSetTest extends AnyWordSpec with Matchers with BeforeAndAfterAll with MockitoSugar {

  val redisServer = new RedisServer(6379)

  override def beforeAll() = redisServer.start()

  override def afterAll() = redisServer.stop()

  "Redis INSERT into Sorted Set (SS) writer" should {

    "write Kafka records to a Redis Sorted Set" in {

      val TOPIC = "cpuTopic"
      val KCQL = s"INSERT INTO cpu_stats SELECT * from $TOPIC STOREAS SortedSet(score=ts)"
      println("Testing KCQL : " + KCQL)
      val props = Map(
        RedisConfigConstants.REDIS_HOST->"localhost",
        RedisConfigConstants.REDIS_PORT->"6379",
        RedisConfigConstants.KCQL_CONFIG->KCQL
      ).asJava

      val config = RedisConfig(props)
      val connectionInfo = new RedisConnectionInfo("localhost", 6379, None)
      val settings = RedisSinkSettings(config)
      val writer = new RedisInsertSortedSet(settings)
      writer.createClient(settings)

      val schema = SchemaBuilder.struct().name("com.example.Cpu")
        .field("type", Schema.STRING_SCHEMA)
        .field("temperature", Schema.FLOAT64_SCHEMA)
        .field("voltage", Schema.FLOAT64_SCHEMA)
        .field("ts", Schema.INT64_SCHEMA).build()

      val struct1 = new Struct(schema).put("type", "Xeon").put("temperature", 60.4).put("voltage", 90.1).put("ts", 1482180657010L)
      val struct2 = new Struct(schema).put("type", "i7").put("temperature", 62.1).put("voltage", 103.3).put("ts", 1482180657020L)
      val struct3 = new Struct(schema).put("type", "i7-i").put("temperature", 64.5).put("voltage", 101.1).put("ts", 1482180657030L)

      val sinkRecord1 = new SinkRecord(TOPIC, 0, null, null, schema, struct1, 1)
      val sinkRecord2 = new SinkRecord(TOPIC, 0, null, null, schema, struct2, 2)
      val sinkRecord3 = new SinkRecord(TOPIC, 0, null, null, schema, struct3, 3)

      val jedis = new Jedis(connectionInfo.host, connectionInfo.port)
      // Clean up in-memory jedis
      jedis.flushAll()

      writer.write(Seq(sinkRecord1))
      writer.write(Seq(sinkRecord2, sinkRecord3))

      // Redis cardinality should now be 3
      jedis.zcard("cpu_stats") shouldBe 3

      val allSSrecords = jedis.zrange("cpu_stats", 0, 999999999999L)
      val results = allSSrecords.asScala.toList
      results.head shouldBe """{"type":"Xeon","temperature":60.4,"voltage":90.1,"ts":1482180657010}"""
      results(1) shouldBe """{"type":"i7","temperature":62.1,"voltage":103.3,"ts":1482180657020}"""
      results(2) shouldBe """{"type":"i7-i","temperature":64.5,"voltage":101.1,"ts":1482180657030}"""

    }

  }

}
