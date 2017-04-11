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

package com.datamountaineer.streamreactor.connect.hazelcast.config

import com.datamountaineer.streamreactor.connect.errors.ThrowErrorPolicy
import com.datamountaineer.streamreactor.connect.hazelcast.TestBase
import com.hazelcast.config.Config
import com.hazelcast.core.{Hazelcast, HazelcastInstance}

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 08/08/16. 
  * stream-reactor
  */
class TestHazelCastSinkSettings extends TestBase {

  var instance : HazelcastInstance = _

  before {
    val configApp1 = new Config()
    configApp1.getGroupConfig.setName(GROUP_NAME).setPassword(HazelCastSinkConfigConstants.SINK_GROUP_PASSWORD_DEFAULT)
    instance = Hazelcast.newHazelcastInstance(configApp1)
  }

  after {
    instance.shutdown()
  }

  "Should build settings object from a config" in {
    val props = getProps
    val config = new HazelCastSinkConfig(props)
    val settings = HazelCastSinkSettings(config)

    settings.topicObject(TOPIC) shouldBe HazelCastStoreAsType(s"${TABLE}_avro", TargetType.RELIABLE_TOPIC)
    settings.ignoreFields(TOPIC).size shouldBe 0
    settings.routes.head.isIncludeAllFields shouldBe true
    settings.errorPolicy.isInstanceOf[ThrowErrorPolicy] shouldBe true
  }

  "Should build settings object from a config with selection" in {
    val props = getPropsSelection
    val config = new HazelCastSinkConfig(props)
    val settings = HazelCastSinkSettings(config)

    settings.topicObject(TOPIC) shouldBe HazelCastStoreAsType(TABLE, TargetType.RELIABLE_TOPIC)
    settings.ignoreFields(TOPIC).size shouldBe 0
    settings.routes.head.isIncludeAllFields shouldBe false
    settings.errorPolicy.isInstanceOf[ThrowErrorPolicy] shouldBe true
    val fields  = settings.fieldsMap(TOPIC)
    fields("a") shouldBe "a"
    fields("b") shouldBe "b"
    fields("c") shouldBe "c"
    fields.getOrElse("d", "e") shouldBe "e"
  }

  "Should build settings object from a config with ignore" in {
    val props = getPropsIgnored
    val config = new HazelCastSinkConfig(props)
    val settings = HazelCastSinkSettings(config)

    settings.topicObject(TOPIC) shouldBe HazelCastStoreAsType(TABLE, TargetType.RELIABLE_TOPIC)
    settings.ignoreFields(TOPIC).size shouldBe 1
    settings.routes.head.getIgnoredField.asScala.toSeq.head shouldBe "a"
    settings.errorPolicy.isInstanceOf[ThrowErrorPolicy] shouldBe true
  }
}
