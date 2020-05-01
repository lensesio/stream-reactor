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

package com.datamountaineer.streamreactor.connect.sink.converters

import java.util.UUID

import com.datamountaineer.streamreactor.connect.jms.config.{JMSConfig, JMSSettings}
import com.datamountaineer.streamreactor.connect.jms.sink.converters.MapMessageConverter
import com.datamountaineer.streamreactor.connect.{TestBase, Using}
import javax.jms.MapMessage
import org.apache.activemq.ActiveMQConnectionFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.JavaConverters._
import scala.reflect.io.Path

class MapMessageConverterTest extends AnyWordSpec with Matchers with Using with BeforeAndAfterAll with TestBase {
  val converter = new MapMessageConverter()

  val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
  val topicName = UUID.randomUUID().toString
  val queueName = UUID.randomUUID().toString
  val kcqlT = getKCQL(topicName, kafkaTopic1, "TOPIC")
  val kcqlQ = getKCQL(queueName, kafkaTopic1, "QUEUE")
  val props = getProps(s"$kcqlQ;$kcqlT", JMS_URL)
  val config = JMSConfig(props.asJava)
  val settings = JMSSettings(config, true)
  val setting = settings.settings.head

  override def afterAll(): Unit = {
    Path(AVRO_FILE).delete()
  }

  "MapMessageConverter" should {
    "create a JMS MapMessage" in {
      val connectionFactory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false")

      using(connectionFactory.createConnection()) { connection =>
        using(connection.createSession(false, 1)) { session =>
          val record = getSinkRecords(kafkaTopic1).head
          val msg = converter.convert(record, session, setting)._2.asInstanceOf[MapMessage]

          Option(msg).isDefined shouldBe true

          msg.getBoolean("boolean") shouldBe true
          msg.getByte("int8") shouldBe 12.toByte
          msg.getShort("int16") shouldBe 12.toShort
          msg.getInt("int32") shouldBe 12
          msg.getLong("int64") shouldBe 12L
          msg.getFloat("float32") shouldBe 12.2f
          msg.getDouble("float64") shouldBe 12.2
          msg.getString("string") shouldBe "foo"
          msg.getBytes("bytes") shouldBe "foo".getBytes()
          val arr = msg.getObject("array")
          arr.asInstanceOf[java.util.List[String]].asScala.toArray shouldBe Array("a", "b", "c")

          val map1 = msg.getObject("map").asInstanceOf[java.util.Map[String, Int]].asScala.toMap
          map1 shouldBe Map("field" -> 1)

          val map2 = msg.getObject("mapNonStringKeys").asInstanceOf[java.util.Map[Int, Int]].asScala.toMap
          map2 shouldBe Map(1 -> 1)

        }
      }
    }
  }
}
