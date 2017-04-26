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

package com.datamountaineer.streamreactor.connect.source

import com.datamountaineer.streamreactor.connect.TestBase
import com.datamountaineer.streamreactor.connect.jms.config.{JMSConfig, JMSConfigConstants}
import com.datamountaineer.streamreactor.connect.jms.source.JMSSourceConnector

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 24/03/2017. 
  * stream-reactor
  */
class JMSSourceConnectorTest extends TestBase {
 "should start a JMS Source Connector" in {
   val props = getPropsMixCDI()
   val connector = new JMSSourceConnector()
   connector.start(props = props)
   val configs = connector.taskConfigs(2)
   val config1 = configs.asScala.head.asScala
   val config2 = configs.asScala.last.asScala
   config1(JMSConfigConstants.KCQL) shouldBe KCQL_SOURCE_QUEUE
   config2(JMSConfigConstants.KCQL) shouldBe KCQL_SOURCE_TOPIC
   connector.stop()
 }
}
