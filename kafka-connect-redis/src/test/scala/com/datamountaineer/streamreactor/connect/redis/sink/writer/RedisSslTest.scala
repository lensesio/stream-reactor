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

import java.net.URI

import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisConfig, RedisConfigConstants, RedisSinkSettings}
import com.google.gson.Gson
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._

/*
README BEFORE THE TEST

Since Redis natively doesn't support ssl connections
we use tunneling via port 6390 and the Jedis client https://github.com/xetorthio/jedis

The test requires to:
1) Start the server by executing `make` on https://github.com/xetorthio/jedis/blob/master/Makefile
2) set the truststoreFilePath below with the location of truststore.jceks file
3) set the runTests to true
*/

class RedisSslTest extends AnyWordSpec with Matchers with BeforeAndAfterAll with MockitoSugar {

  val runTests = false;

  val truststoreFilePath = "src/test/resources/truststore.jceks"

  val gson = new Gson()

  val TOPIC = "topic"
  val baseProps = Map(
    RedisConfigConstants.REDIS_HOST -> "localhost",
    RedisConfigConstants.REDIS_PORT -> "6390",
    RedisConfigConstants.REDIS_PASSWORD -> "foobared",
    RedisConfigConstants.REDIS_SSL_ENABLED -> "true",
    SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG -> truststoreFilePath
  )

//  def setupTrustStore(): Unit = {
//    setJvmTrustStore(truststoreFilePath, "jceks")
//  }

//  private def setJvmTrustStore(trustStoreFilePath: String, trustStoreType: String): Unit = {
//    new File(trustStoreFilePath).exists shouldBe true
//    System.setProperty("javax.net.ssl.trustStore", trustStoreFilePath)
//    System.setProperty("javax.net.ssl.trustStoreType", trustStoreType)
//  }

//  override def beforeAll() = {
//    if (runTests) {
//      setupTrustStore()
//    }
//  }
//
//  override def afterAll() = {
//  }

  "JedisSslClient" should {

    "establish ssl connection" in {

      val truststoreFilePath = getClass.getResource("/truststore.jks").getPath
      val keystoreFilePath = getClass.getResource("/keystore.jks").getPath

      val map = Map(RedisConfigConstants.REDIS_HOST -> "rediss://localhost",
        RedisConfigConstants.REDIS_PORT -> "8453",
        RedisConfigConstants.KCQL_CONFIG -> "SELECT * FROM topicA PK firstName, child.firstName",
        RedisConfigConstants.ERROR_POLICY -> "THROW",
        RedisConfigConstants.REDIS_SSL_ENABLED -> "true",
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG -> truststoreFilePath,
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG -> "truststore-password",
        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG -> keystoreFilePath,
        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG -> "keystore-password")

      val config =  RedisConfig(map.asJava)
      val settings = RedisSinkSettings(config)


      val writer = new RedisCache(settings)
      writer.createClient(settings)

      val props = System.getProperties
      props.containsKey("javax.net.ssl.keyStorePassword") shouldBe true
      props.get("javax.net.ssl.keyStorePassword") shouldBe "keystore-password"
      props.containsKey("javax.net.ssl.keyStore") shouldBe true
      props.get("javax.net.ssl.keyStore") shouldBe keystoreFilePath
      props.containsKey("javax.net.ssl.keyStoreType") shouldBe true
      props.get("javax.net.ssl.keyStoreType") shouldBe "JKS"

      props.containsKey("javax.net.ssl.trustStorePassword") shouldBe true
      props.get("javax.net.ssl.trustStorePassword") shouldBe "truststore-password"
      props.containsKey("javax.net.ssl.trustStore") shouldBe true
      props.get("javax.net.ssl.trustStore") shouldBe truststoreFilePath
      props.containsKey("javax.net.ssl.trustStoreType") shouldBe true
      props.get("javax.net.ssl.trustStoreType") shouldBe "JKS"

//      if (runTests) {
//
//        val jedis = new Jedis(URI.create(s"rediss://${baseProps(RedisConfigConstants.REDIS_HOST)}:${baseProps(RedisConfigConstants.REDIS_PORT)}"))
//        jedis.auth(baseProps(RedisConfigConstants.REDIS_PASSWORD))
//        jedis.ping() shouldBe "PONG"
//
//      }
    }
  }

  "RedisDbWriter" should {

    "write Kafka records to Redis using CACHE mode and ssl connection" in {

      if (runTests) {

        val jedis = new Jedis(URI.create(s"rediss://${baseProps(RedisConfigConstants.REDIS_HOST)}:${baseProps(RedisConfigConstants.REDIS_PORT)}"))
        jedis.auth(baseProps(RedisConfigConstants.REDIS_PASSWORD))
        jedis.ping() shouldBe "PONG"

        val QUERY_ALL = s"SELECT * FROM $TOPIC PK firstName, child.firstName"
        val props = (baseProps + (RedisConfigConstants.KCQL_CONFIG -> QUERY_ALL)).asJava
        val config = RedisConfig(props)
        val settings = RedisSinkSettings(config)
        val writer = new RedisCache(settings)

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
    }

  }
}
