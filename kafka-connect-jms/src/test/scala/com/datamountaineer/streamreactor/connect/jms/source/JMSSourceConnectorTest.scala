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

package com.datamountaineer.streamreactor.connect.jms.source

import com.datamountaineer.streamreactor.connect.jms.TestBase
import com.datamountaineer.streamreactor.connect.jms.config.JMSConfigConstants
import org.scalatest.BeforeAndAfterAll

import java.util.UUID
import scala.jdk.CollectionConverters.{ListHasAsScala, MapHasAsJava, MapHasAsScala}
import scala.reflect.io.Path

/**
 * Created by andrew@datamountaineer.com on 24/03/2017.
 * stream-reactor
 */
class JMSSourceConnectorTest extends TestBase with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    val _ = Path(AVRO_FILE).delete()
  }

  "should start a JMS Source Connector" in {
    val kafkaTopic = s"kafka-${UUID.randomUUID().toString}"
    val queueName = UUID.randomUUID().toString
    val topicName = UUID.randomUUID().toString

    val kcqlT = getKCQL(kafkaTopic, topicName, "TOPIC")
    val kcqlQ = getKCQL(kafkaTopic, queueName, "QUEUE")
    val props = getProps(s"$kcqlQ;$kcqlT", "")

    val connector = new JMSSourceConnector()
    connector.start(props = props.asJava)
    val configs = connector.taskConfigs(3)
    val config1 = configs.asScala.head.asScala
    val config2 = configs.asScala.last.asScala
    config1(JMSConfigConstants.KCQL) shouldBe kcqlQ
    config2(JMSConfigConstants.KCQL) shouldBe kcqlT
    connector.stop()
  }

  "should use the tasks.max to do parallelization" in {
    val kafkaTopic = s"kafka-${UUID.randomUUID().toString}"
    val queueName = UUID.randomUUID().toString
    val topicName = UUID.randomUUID().toString

    val kcqlT = getKCQL(kafkaTopic, topicName, "TOPIC")
    val kcqlQ = getKCQL(kafkaTopic, queueName, "QUEUE")
    val kcql = kcqlQ + ";" + kcqlT
    val props = getProps(kcql, "") + (JMSConfigConstants.TASK_PARALLELIZATION_TYPE -> "default")

    val connector = new JMSSourceConnector()
    connector.start(props = props.asJava)
    val _ = connector.taskConfigs(3).asScala.toList match {
      case l@List(_, _, _) =>
        l.foreach {
          _.get(JMSConfigConstants.KCQL) shouldEqual kcql
        }
      case _ => fail("Invalid task parallelization")
    }
  }
}
