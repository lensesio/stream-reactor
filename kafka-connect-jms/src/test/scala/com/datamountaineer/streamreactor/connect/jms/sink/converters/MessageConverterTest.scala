/*
 *
 *  * Copyright 2020 Lenses.io.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.datamountaineer.streamreactor.connect.jms.sink.converters

import com.datamountaineer.streamreactor.connect.jms.config.{JMSConfig, JMSSettings}
import com.datamountaineer.streamreactor.connect.jms.sink.AvroDeserializer
import com.datamountaineer.streamreactor.connect.jms.{TestBase, Using}
import io.confluent.connect.avro.AvroData
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.ByteBuffer
import java.util.UUID
import javax.jms.{BytesMessage, MapMessage, ObjectMessage, TextMessage}
import scala.collection.JavaConverters._
import scala.reflect.io.Path

class MessageConverterTest extends AnyWordSpec with Matchers with Using with TestBase with BeforeAndAfterAll {
  val converter = new AvroMessageConverter()

  override def afterAll(): Unit = {
    Path(AVRO_FILE).delete()
  }

  val connectionFactory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false")

  "ObjectMessageConverter" should {
    "create an instance of jms ObjectMessage" in {
      val converter = new ObjectMessageConverter()
      val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
      val topicName = UUID.randomUUID().toString
      val queueName = UUID.randomUUID().toString
      val kcqlT = getKCQL(topicName, kafkaTopic1, "TOPIC")
      val kcqlQ = getKCQL(queueName, kafkaTopic1, "QUEUE")
      val props = getProps(s"$kcqlQ;$kcqlT", JMS_URL)
      val config = JMSConfig(props.asJava)
      val settings = JMSSettings(config, true)
      val setting = settings.settings.head

      val connectionFactory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false")

      using(connectionFactory.createConnection()) { connection =>
        using(connection.createSession(false, 1)) { session =>
          val record = getSinkRecords(kafkaTopic1).head
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

  "MapMessageConverter" should {
    "create a JMS MapMessage" in {
      val connectionFactory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false")
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

  "JsonMessageConverter" should {
    "create a TextMessage with Json payload" in {

      val converter = new JsonMessageConverter()

      val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
      val queueName = UUID.randomUUID().toString
      val kcql = getKCQL(queueName, kafkaTopic1, "QUEUE")
      val props = getProps(kcql, JMS_URL)
      val config = JMSConfig(props.asJava)
      val settings = JMSSettings(config, true)
      val setting = settings.settings.head

      val connectionFactory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false")

      using(connectionFactory.createConnection()) { connection =>
        using(connection.createSession(false, 1)) { session =>
          val schema = getSchema
          val struct = getStruct(schema)

          val record = new SinkRecord(kafkaTopic1, 0, null, null, schema, struct, 1)
          val msg = converter.convert(record, session, setting)._2.asInstanceOf[TextMessage]
          Option(msg).isDefined shouldBe true

          val json = msg.getText
          json shouldBe
            """{"int8":12,"int16":12,"int32":12,"int64":12,"float32":12.2,"float64":12.2,"boolean":true,"string":"foo","bytes":"Zm9v","array":["a","b","c"],"map":{"field":1},"mapNonStringKeys":[[1,1]]}""".stripMargin

        }
      }
    }
  }

  "AvroMessageConverter" should {
    "create a BytesMessage with avro payload" in {


      lazy val avroData = new AvroData(128)
      val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
      val topicName = UUID.randomUUID().toString
      val queueName = UUID.randomUUID().toString

      val kcqlT = getKCQL(topicName, kafkaTopic1, "TOPIC")
      val kcqlQ = getKCQL(queueName, kafkaTopic1, "QUEUE")

      val props = getProps(s"$kcqlQ;$kcqlT", JMS_URL)
      val config = JMSConfig(props.asJava)
      val settings = JMSSettings(config, true)
      val setting = settings.settings.head
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

  "ProtoMessageConverter" should {
    "create a BytesMessage with sinkrecord payload when storedAs is null" in {
      val converter = new ProtoMessageConverter()
      val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
      val queueName = UUID.randomUUID().toString
      val kcql = getKCQL(queueName, kafkaTopic1, "QUEUE")
      val props = getProps(kcql, JMS_URL)
      val config = JMSConfig(props.asJava)
      val settings = JMSSettings(config, true)
      val setting = settings.settings.head
      val connectionFactory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false")
      using(connectionFactory.createConnection()) { connection =>
        using(connection.createSession(false, 1)) { session =>
          val schema = getSchema
          val struct = getStruct(schema)
          val record = new SinkRecord(kafkaTopic1, 0, null, null, schema, struct, 1)

          val msg = converter.convert(record, session, setting)._2.asInstanceOf[BytesMessage]

          var byteData: Array[Byte] = null
          msg.reset()
          byteData = new Array[Byte](msg.getBodyLength.asInstanceOf[Int])
          msg.readBytes(byteData)
          val stringMessage = new String(byteData, "UTF-8")

          Option(msg).isDefined shouldBe true
          stringMessage.contains("foo") shouldBe true
        }
      }
    }

    "create a BytesMessage with sinkrecord payload with storedAs data" in {
      val converter = new ProtoMessageConverter()

      val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
      val queueName = UUID.randomUUID().toString
      val kcql = getKCQLStoredAsWithNameOnly(queueName, kafkaTopic1, "QUEUE")
      val props = getProps(kcql, JMS_URL)
      val schema = getProtobufSchema
      val struct = getProtobufStruct(schema, "addrressed-person", 103, "addressed-person@gmail.com")
      val config = JMSConfig(props.asJava)
      val settings = JMSSettings(config, true)
      val connectionFactory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false")
      using(connectionFactory.createConnection()) { connection =>
        using(connection.createSession(false, 1)) { session =>
          val setting = settings.settings.head

          converter.initialize(props.asJava)
          val record = new SinkRecord(kafkaTopic1, 0, null, null, schema, struct, 1)

          val convertedValue = converter.convert(record, session, setting)._2.asInstanceOf[BytesMessage]

          var byteData: Array[Byte] = null
          convertedValue.reset()
          byteData = new Array[Byte](convertedValue.getBodyLength.asInstanceOf[Int])
          convertedValue.readBytes(byteData)
          val stringMessage = new String(byteData, "UTF-8")

          Option(stringMessage).isDefined shouldBe true
          stringMessage.contains("name") shouldBe true
          stringMessage.contains("addressed-person@gmail.com") shouldBe true
          stringMessage.contains("id") shouldBe true
          stringMessage.contains("103") shouldBe true
          stringMessage.contains("email") shouldBe true
          stringMessage.contains("addressed-person@gmail.com") shouldBe true
        }
      }
    }
  }

}
