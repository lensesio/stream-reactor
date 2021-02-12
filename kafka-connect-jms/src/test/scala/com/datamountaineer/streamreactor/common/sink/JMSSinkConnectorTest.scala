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

package com.datamountaineer.streamreactor.common.sink

import java.util.UUID

import com.datamountaineer.streamreactor.common.TestBase
import com.datamountaineer.streamreactor.common.jms.sink.JMSSinkConnector
import org.scalatest.BeforeAndAfterAll

import scala.reflect.io.Path

/**
  * Created by andrew@datamountaineer.com on 24/03/2017. 
  * stream-reactor
  */
class JMSSinkConnectorTest extends TestBase  with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    Path(AVRO_FILE).delete()
  }

  "should start a JMSSinkConnector" in {
    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val queueName = UUID.randomUUID().toString
    val kcql = getKCQL(queueName, kafkaTopic1, "QUEUE")
    val props = getSinkProps(kcql, kafkaTopic1, "")
    val connector = new JMSSinkConnector()
    connector.start(props)
    val configs = connector.taskConfigs(1)
    configs.size() shouldBe 1
  }
}
