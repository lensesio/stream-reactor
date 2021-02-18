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
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import redis.clients.jedis.{Jedis, JedisPubSub}
import redis.embedded.RedisServer

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class RedisPubSubTest extends AnyWordSpec with Matchers with BeforeAndAfterAll with MockitoSugar with LazyLogging {

  val redisServer = new RedisServer(6379)

  override def beforeAll() = redisServer.start()

  override def afterAll() = redisServer.stop()

  "Redis PUBSUB writer" should {

    "write Kafka records to a Redis PubSub" in {

      val TOPIC = "cpuTopic"
      val KCQL = s"SELECT * from $TOPIC STOREAS PubSub (channel=type)"
      println("Testing KCQL : " + KCQL)
      val props = Map(
        RedisConfigConstants.REDIS_HOST -> "localhost",
        RedisConfigConstants.REDIS_PORT -> "6379",
        RedisConfigConstants.KCQL_CONFIG -> KCQL
      ).asJava

      val config = RedisConfig(props)
      val connectionInfo = new RedisConnectionInfo("localhost", 6379, None)
      val settings = RedisSinkSettings(config)
      val writer = new RedisPubSub(settings)
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

      val messagesMap = collection.mutable.Map[String, ListBuffer[String]]()

      val t = new Thread {
        private val pubsub = new JedisPubSub {
          override def onMessage(channel: String, message: String): Unit = {
            messagesMap.get(channel) match {
              case Some(msgs) => {
                logger.info("Receiving message!")
                messagesMap.put(channel, msgs += message)
              }
              case None => messagesMap.put(channel, ListBuffer(message))
            }
          }
        }

        override def run(): Unit = {
          jedis.subscribe(pubsub, "Xeon", "i7", "i7-i")
        }

        override def interrupt(): Unit = {
          pubsub.punsubscribe("*")
          super.interrupt()
        }
      }
      t.start()
      t.join(5000)
      if (t.isAlive) t.interrupt()

      writer.write(Seq(sinkRecord1))
      writer.write(Seq(sinkRecord2, sinkRecord3))

      eventually {
        messagesMap.size shouldBe 3

        messagesMap("Xeon").head shouldBe """{"type":"Xeon","temperature":60.4,"voltage":90.1,"ts":1482180657010}"""
        messagesMap("i7").head shouldBe """{"type":"i7","temperature":62.1,"voltage":103.3,"ts":1482180657020}"""
        messagesMap("i7-i").head shouldBe """{"type":"i7-i","temperature":64.5,"voltage":101.1,"ts":1482180657030}"""
      }

    }
  }
}
