/*
 * Copyright 2017-2023 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.gcp.storage.sink

import java.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GCPStorageSinkConnectorTest extends AnyFlatSpec with Matchers {

  "taskClass" should "return the correct task class" in {
    val connector = new GCPStorageSinkConnector
    val taskClass = connector.taskClass()
    taskClass shouldEqual classOf[GCPStorageSinkTask]
  }

  "config" should "return the correct config definition" in {
    val connector = new GCPStorageSinkConnector
    val configDef = connector.config()
    configDef should not be null
  }

  "stop" should "stop without errors" in {
    val connector = new GCPStorageSinkConnector
    connector.stop() // No assertion, just checking for no exceptions
  }

  "start" should "distribute the provided properties to appropriate number of tasks" in {
    val connector = new GCPStorageSinkConnector
    val props     = new util.HashMap[String, String]()
    props.put("key", "value")
    connector.start(props)
    val taskConfigs = connector.taskConfigs(2)
    taskConfigs should have size 2
    taskConfigs.get(0).get("key") shouldEqual "value"
    taskConfigs.get(1).get("key") shouldEqual "value"
  }

}
