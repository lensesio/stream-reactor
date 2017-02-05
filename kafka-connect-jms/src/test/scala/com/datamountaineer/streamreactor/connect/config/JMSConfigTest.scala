/*
 *  Copyright 2016 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.config


import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.jms.sink.config.{JMSConfig, QueueDestination, TopicDestination}
import io.confluent.common.config.ConfigException
import org.scalatest.{Matchers, WordSpec}

class JMSConfigTest extends WordSpec with Matchers {
  "JMSConfig" should {
    "should create a setting for JMS queue" in {
      val queue = "queue1"
      val config = Config.parse(s"INSERT INTO $queue SELECT * FROM topica")
      val jms = JMSConfig(config, Set.empty[String], Set(queue))
      jms.destinationType shouldBe QueueDestination
    }

    "should create a setting for JMS topics" in {
      val queue = "queue1"
      val topic = "topic1"
      val config = Config.parse(s"INSERT INTO $topic SELECT * FROM $topic")
      val jms = JMSConfig(config, Set(topic), Set(queue))
      jms.destinationType shouldBe TopicDestination
    }

    "should throw an exception if the target can not be found in either topics or queues " in {
      intercept[ConfigException] {
        val queue = "queue1"
        val topic = "topic1"
        val config = Config.parse(s"INSERT INTO notfound SELECT * FROM $topic")
        val jms = JMSConfig(config, Set(topic), Set(queue))
        jms.destinationType shouldBe TopicDestination
      }
    }

    "should create an instance of JMSConfig" in {
      val queue = "queue1"
      val topic = "topic1"
      val source = "source1Topic"
      val config = Config.parse(s"INSERT INTO $topic SELECT * FROM $source AUTOCREATE")
      val jms = JMSConfig(config, Set(topic), Set(queue))
      jms.destinationType shouldBe TopicDestination
      jms.fieldsAlias shouldBe Map.empty
      jms.source shouldBe source
      jms.target shouldBe topic
    }
  }
}
