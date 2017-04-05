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

import javax.jms.ObjectMessage

import com.datamountaineer.streamreactor.connect.TestBase
import com.datamountaineer.streamreactor.connect.jms.config.{JMSConfig, JMSSettings}
import com.datamountaineer.streamreactor.connect.jms.sink.converters.ObjectMessageConverter
import com.sksamuel.scalax.io.Using
import org.apache.activemq.ActiveMQConnectionFactory
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class ObjectMessageConverterTest extends WordSpec with Matchers with Using with TestBase {
  val converter = new ObjectMessageConverter()
  val props = getPropsMixJNDIWithSink()
  val config = JMSConfig(props)
  val settings = JMSSettings(config, true)
  val setting = settings.settings.head

  "ObjectMessageConverter" should {
    "create an instance of jms ObjectMessage" in {
      val connectionFactory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false")

      using(connectionFactory.createConnection()) { connection =>
        using(connection.createSession(false, 1)) { session =>
          val record = getSinkRecords.head
          val msg = converter.convert(record, session, setting)._2.asInstanceOf[ObjectMessage]

          Option(msg).isDefined shouldBe true

          msg.getBooleanProperty("boolean") shouldBe true
          msg.getByteProperty("int8") shouldBe 12.toByte
          msg.getShortProperty("int16") shouldBe 12.toShort
          msg.getIntProperty("int32") shouldBe 12
          msg.getLongProperty("int64") shouldBe 12L
          msg.getFloatProperty("float32") shouldBe 12.2f
          msg.getDoubleProperty("float64") shouldBe 12.2
          msg.getStringProperty("string") shouldBe "foo"
          msg.getObjectProperty("bytes").asInstanceOf[java.util.List[Byte]].toArray shouldBe "foo".getBytes()
          val arr = msg.getObjectProperty("array")
          arr.asInstanceOf[java.util.List[String]].asScala.toArray shouldBe Array("a", "b", "c")

          val map1 = msg.getObjectProperty("map").asInstanceOf[java.util.Map[String, Int]].asScala.toMap
          map1 shouldBe Map("field" -> 1)

          val map2 = msg.getObjectProperty("mapNonStringKeys").asInstanceOf[java.util.Map[Int, Int]].asScala.toMap
          map2 shouldBe Map(1 -> 1)
        }
      }
    }
  }
}
