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

package io.lenses.streamreactor.connect.redis.sink.writer

import io.lenses.streamreactor.connect.redis.sink.config.RedisConfig
import io.lenses.streamreactor.connect.redis.sink.config.RedisConfigConstants
import io.lenses.streamreactor.connect.redis.sink.config.RedisSinkSettings
import com.dimafeng.testcontainers.ForAllTestContainer
import com.dimafeng.testcontainers.GenericContainer
import com.google.gson.Gson
import io.lenses.streamreactor.connect.redis.sink.JedisClientBuilder
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import redis.clients.jedis.Jedis

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala

class RedisCacheTest
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with MockitoSugar
    with ForAllTestContainer {

  private val ContainerPort = 6379

  override val container = GenericContainer(
    dockerImage  = "redis:6-alpine",
    exposedPorts = Seq(ContainerPort),
  )

  val gson  = new Gson()
  val TOPIC = "topic"

  "RedisDbWriter" should {
    class BasePropsContext {
      val containerPort = container.mappedPort(ContainerPort)
      val jedis         = new Jedis("localhost", containerPort)
      val baseProps = Map(
        RedisConfigConstants.REDIS_HOST -> "localhost",
        RedisConfigConstants.REDIS_PORT -> containerPort.toString,
      )
    }
    "write Kafka records to Redis using CACHE mode with JSON no schema" in new BasePropsContext {
      val QUERY_ALL = s"SELECT * FROM $TOPIC PK firstName, child.firstName"
      val props     = (baseProps + (RedisConfigConstants.KCQL_CONFIG -> QUERY_ALL)).asJava
      val config    = RedisConfig(props)
      val settings  = RedisSinkSettings(config)
      val writer    = new RedisCache(settings, JedisClientBuilder.createClient(settings))

      val json =
        """
          |{
          |   "firstName":"Alex",
          |   "age":30.0,
          |   "child": {
          |     "firstName": "Alex_Junior"
          |   }
          |}
        """.stripMargin

      val record =
        new SinkRecord(TOPIC, 0, null, null, Schema.STRING_SCHEMA, json, 0)

      writer.write(Seq(record))

      val alexValue = jedis.get("Alex.Alex_Junior")
      alexValue should not be null

      val alexMap = gson.fromJson(alexValue, classOf[java.util.Map[String, AnyRef]]).asScala
      alexMap("firstName").toString shouldBe "Alex"
      alexMap("age").toString shouldBe "30.0" //it gets back a java double!?
      alexMap("child").asInstanceOf[java.util.Map[String, AnyRef]].get("firstName") shouldBe "Alex_Junior"
    }

    "write Kafka records to Redis using CACHE mode" in new BasePropsContext {
      val QUERY_ALL = s"SELECT * FROM $TOPIC PK firstName, child.firstName"
      val props     = (baseProps + (RedisConfigConstants.KCQL_CONFIG -> QUERY_ALL)).asJava
      val config    = RedisConfig(props)
      val settings  = RedisSinkSettings(config)
      val writer    = new RedisCache(settings, JedisClientBuilder.createClient(settings))

      val childSchema = SchemaBuilder.struct().name("com.example.Child")
        .field("firstName", Schema.STRING_SCHEMA)
        .build()

      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("child", childSchema)
        .build()

      val alexJr = new Struct(childSchema)
        .put("firstName", "Alex_Junior")
      val alex = new Struct(schema)
        .put("firstName", "Alex")
        .put("age", 30)
        .put("child", alexJr)
      val maraJr = new Struct(childSchema)
        .put("firstName", "Mara_Junior")
      val mara = new Struct(schema).put("firstName", "Mara")
        .put("age", 22)
        .put("threshold", 12.4)
        .put("child", maraJr)

      val alexRecord = new SinkRecord(TOPIC, 1, null, null, schema, alex, 0)
      val maraRecord = new SinkRecord(TOPIC, 1, null, null, schema, mara, 1)

      writer.write(Seq(alexRecord, maraRecord))

      val alexValue = jedis.get("Alex.Alex_Junior")
      alexValue should not be null

      val alexMap = gson.fromJson(alexValue, classOf[java.util.Map[String, AnyRef]]).asScala
      alexMap("firstName").toString shouldBe "Alex"
      alexMap("age").toString shouldBe "30.0" //it gets back a java double!?
      alexMap("child").asInstanceOf[java.util.Map[String, AnyRef]].get("firstName") shouldBe "Alex_Junior"

      val maraValue = jedis.get("Mara.Mara_Junior")
      maraValue should not be null

      val maraMap = gson.fromJson(maraValue, classOf[java.util.Map[String, AnyRef]]).asScala
      maraMap("firstName") shouldBe "Mara"
      maraMap("age").toString shouldBe "22.0"
      maraMap("threshold").toString shouldBe "12.4"
      maraMap("child").asInstanceOf[java.util.Map[String, AnyRef]].get("firstName") shouldBe "Mara_Junior"
    }

    "write Kafka records to Redis using CACHE mode and PK field is not in the selected fields" in new BasePropsContext {
      val QUERY_ALL = s"SELECT age FROM $TOPIC PK firstName"
      val props     = (baseProps + (RedisConfigConstants.KCQL_CONFIG -> QUERY_ALL)).asJava
      val config    = RedisConfig(props)
      val settings  = RedisSinkSettings(config)
      val writer    = new RedisCache(settings, JedisClientBuilder.createClient(settings))

      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct1 = new Struct(schema).put("firstName", "Alex").put("age", 30)
      val struct2 = new Struct(schema).put("firstName", "Mara").put("age", 22).put("threshold", 12.4)

      val sinkRecord1 = new SinkRecord(TOPIC, 1, null, null, schema, struct1, 0)
      val sinkRecord2 = new SinkRecord(TOPIC, 1, null, null, schema, struct2, 1)

      writer.write(Seq(sinkRecord1, sinkRecord2))

      val val1 = jedis.get("Alex")
      val1 should not be null

      val map1 = gson.fromJson(val1, classOf[java.util.Map[String, AnyRef]]).asScala
      map1("age").toString shouldBe "30.0" //it gets back a java double!?

      val val2 = jedis.get("Mara")
      val2 should not be null

      val map2 = gson.fromJson(val2, classOf[java.util.Map[String, AnyRef]]).asScala
      map2("age").toString shouldBe "22.0"
    }

    "write Kafka records to Redis using CACHE mode with explicit KEY (using INSERT)" in new BasePropsContext {
      val TOPIC = "topic2"
      val KCQL  = s"INSERT INTO KEY_PREFIX_ SELECT * FROM $TOPIC PK firstName"
      val props = (baseProps + (RedisConfigConstants.KCQL_CONFIG -> KCQL)).asJava

      val config   = RedisConfig(props)
      val settings = RedisSinkSettings(config)
      val writer   = new RedisCache(settings, JedisClientBuilder.createClient(settings))

      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct1 = new Struct(schema).put("firstName", "Alex").put("age", 30)
      val struct2 = new Struct(schema).put("firstName", "Mara").put("age", 22).put("threshold", 12.4)

      val sinkRecord1 = new SinkRecord(TOPIC, 1, null, null, schema, struct1, 0)
      val sinkRecord2 = new SinkRecord(TOPIC, 1, null, null, schema, struct2, 1)

      writer.write(Seq(sinkRecord1, sinkRecord2))

      val val1 = jedis.get("KEY_PREFIX_Alex")
      val1 should not be null

      val map1 = gson.fromJson(val1, classOf[java.util.Map[String, AnyRef]]).asScala
      map1("firstName").toString shouldBe "Alex"
      map1("age").toString shouldBe "30.0" //it gets back a java double!?

      val val2 = jedis.get("KEY_PREFIX_Mara")
      val2 should not be null

      val map2 = gson.fromJson(val2, classOf[java.util.Map[String, AnyRef]]).asScala
      map2("firstName") shouldBe "Mara"
      map2("age").toString shouldBe "22.0"
      map2("threshold").toString shouldBe "12.4"
    }
  }

  "RedisDbWriter" should {
    class BasePropsContext {
      val containerPort = container.mappedPort(ContainerPort)
      val jedis         = new Jedis("localhost", containerPort)

      val baseProps = Map(
        RedisConfigConstants.REDIS_HOST -> "localhost",
        RedisConfigConstants.REDIS_PORT -> container.mappedPort(ContainerPort).toString,
      )
      val QUERY_ALL  = s"SELECT * FROM $TOPIC PK firstName, child.firstName"
      val base_Props = baseProps + (RedisConfigConstants.KCQL_CONFIG -> QUERY_ALL)

      val childSchema = SchemaBuilder.struct().name("com.example.Child")
        .field("firstName", Schema.STRING_SCHEMA)
        .build()

      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("child", childSchema)
        .build()

      val nickJr = new Struct(childSchema)
        .put("firstName", "Nick_Junior")
      val nick = new Struct(schema)
        .put("firstName", "Nick")
        .put("age", 30)
        .put("child", nickJr)

      val nickRecord = new SinkRecord(TOPIC, 1, null, null, schema, nick, 0)

    }

    "write Kafka records to Redis using CACHE mode and PK has default delimiter" in new BasePropsContext {

      val props    = base_Props.asJava
      val config   = RedisConfig(props)
      val settings = RedisSinkSettings(config)
      val writer   = new RedisCache(settings, JedisClientBuilder.createClient(settings))

      writer.write(Seq(nickRecord))

      val key = nick.get("firstName").toString + RedisConfigConstants.REDIS_PK_DELIMITER_DEFAULT_VALUE + nickJr.get(
        "firstName",
      ).toString
      val nickValue = jedis.get(key)
      key shouldBe "Nick.Nick_Junior"
      nickValue should not be null
    }

    "write Kafka records to Redis using CACHE mode and PK has custom delimiter" in new BasePropsContext {

      val delimiter = "-"
      val props     = (base_Props + (RedisConfigConstants.REDIS_PK_DELIMITER -> delimiter)).asJava
      val config    = RedisConfig(props)
      val settings  = RedisSinkSettings(config)
      val writer    = new RedisCache(settings, JedisClientBuilder.createClient(settings))

      writer.write(Seq(nickRecord))

      val key       = nick.get("firstName").toString + delimiter + nickJr.get("firstName").toString
      val nickValue = jedis.get(key)
      key shouldBe "Nick-Nick_Junior"
      nickValue should not be null
    }

    "write Kafka records to Redis using CACHE mode and PK has custom delimiter but not set" in new BasePropsContext {

      val delimiter = "$"
      val props     = base_Props.asJava
      val config    = RedisConfig(props)
      val settings  = RedisSinkSettings(config)
      val writer    = new RedisCache(settings, JedisClientBuilder.createClient(settings))

      writer.write(Seq(nickRecord))

      val key       = nick.get("firstName").toString + delimiter + nickJr.get("firstName").toString
      val nickValue = jedis.get(key)
      key shouldBe "Nick$Nick_Junior"
      nickValue shouldBe null
    }
  }
}
