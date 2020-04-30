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

import java.nio.ByteBuffer
import java.util.UUID

import com.datamountaineer.streamreactor.connect.jms.config.{JMSConfig, JMSSettings}
import com.datamountaineer.streamreactor.connect.jms.sink.converters.AvroMessageConverter
import com.datamountaineer.streamreactor.connect.sink.AvroDeserializer
import com.datamountaineer.streamreactor.connect.{TestBase, Using}
import io.confluent.connect.avro.AvroData
import javax.jms.BytesMessage
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.JavaConverters._
import scala.reflect.io.Path

class AvroMessageConverterTest extends AnyWordSpec with Matchers with Using with TestBase with BeforeAndAfterAll {
  val converter = new AvroMessageConverter()
  private lazy val avroData = new AvroData(128)
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

  "AvroMessageConverter" should {
    "create a BytesMessage with avro payload" in {
      val connectionFactory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false")

      using(connectionFactory.createConnection()) { connection =>
        using(connection.createSession(false, 1)) { session =>

          val record = getSinkRecords(kafkaTopic1).head
          val msg = converter.convert(record, session, setting)._2.asInstanceOf[BytesMessage]

          Option(msg).isDefined shouldBe true

          msg.reset()

          val size = msg.getBodyLength

          size > 0 shouldBe true
          val data = new Array[Byte](size.toInt)
          msg.readBytes(data)

          val avroRecord = AvroDeserializer(data, avroData.fromConnectSchema(record.valueSchema()))
          avroRecord.get("int8") shouldBe 12.toByte
          avroRecord.get("int16") shouldBe 12.toShort
          avroRecord.get("int32") shouldBe 12
          avroRecord.get("int64") shouldBe 12L
          avroRecord.get("float32") shouldBe 12.2f
          avroRecord.get("float64") shouldBe 12.2
          avroRecord.get("boolean") shouldBe true
          avroRecord.get("string").toString shouldBe "foo"
          avroRecord.get("bytes").asInstanceOf[ByteBuffer].array() shouldBe "foo".getBytes()
          val array = avroRecord.get("array").asInstanceOf[GenericData.Array[Utf8]]
          val iter = array.iterator()
          new Iterator[String] {
            override def hasNext: Boolean = iter.hasNext

            override def next(): String = iter.next().toString
          }.toSeq shouldBe Seq("a", "b", "c")
          val map = avroRecord.get("map").asInstanceOf[java.util.Map[Utf8, Int]].asScala
          map.size shouldBe 1
          map.keys.head.toString shouldBe "field"
          map.get(map.keys.head) shouldBe Some(1)

          val iterRecord = avroRecord.get("mapNonStringKeys").asInstanceOf[GenericData.Array[GenericData.Record]].iterator()
          iterRecord.hasNext shouldBe true
          val r = iterRecord.next()
          r.get("key") shouldBe 1
          r.get("value") shouldBe 1
        }
      }
    }
  }
}
