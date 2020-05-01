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
import com.datamountaineer.streamreactor.connect.jms.sink.converters.JsonMessageConverter
import com.datamountaineer.streamreactor.connect.{TestBase, Using}
import javax.jms.TextMessage
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.JavaConverters._
import scala.reflect.io.Path


class JsonMessageConverterTest extends AnyWordSpec with Matchers with Using with TestBase with BeforeAndAfterAll {

  val converter = new JsonMessageConverter()

  val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
  val queueName = UUID.randomUUID().toString
  val kcql = getKCQL(queueName, kafkaTopic1, "QUEUE")
  val props = getProps(kcql, JMS_URL)
  val config = JMSConfig(props.asJava)
  val settings = JMSSettings(config, true)
  val setting = settings.settings.head

  override def afterAll(): Unit = {
    Path(AVRO_FILE).delete()
  }

  "JsonMessageConverter" should {
    "create a TextMessage with Json payload" in {
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
}