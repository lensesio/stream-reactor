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

package com.datamountaineer.streamreactor.connect.hazelcast.sink

import com.datamountaineer.streamreactor.connect.hazelcast.TestBase
import com.datamountaineer.streamreactor.connect.hazelcast.config.{HazelCastSinkConfig, HazelCastSinkConfigConstants}

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 10/08/16. 
  * stream-reactor
  */
class TestHazelCastSinkConnector extends TestBase {
  "should start a Hazelcast sink connector" in {
    val props = getProps
    val connector = new HazelCastSinkConnector
    connector.start(props)
    val taskConfigs = connector.taskConfigs(1)
    taskConfigs.asScala.head.get(HazelCastSinkConfigConstants.EXPORT_ROUTE_QUERY) shouldBe EXPORT_MAP
    taskConfigs.asScala.head.get(HazelCastSinkConfigConstants.SINK_GROUP_NAME) shouldBe GROUP_NAME
    connector.stop()
  }
}
