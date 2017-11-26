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

package com.datamountaineer.streamreactor.connect.config

import com.datamountaineer.streamreactor.connect.TestBase
import com.datamountaineer.streamreactor.connect.converters.source.AvroConverter
import com.datamountaineer.streamreactor.connect.jms.JMSSessionProvider
import com.datamountaineer.streamreactor.connect.jms.config._
import org.apache.kafka.common.config.ConfigException
import org.scalatest.BeforeAndAfterAll

import scala.reflect.io.Path


class JMSSettingsTest extends TestBase with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    Path(AVRO_FILE).delete()
  }

  "should create a JMSSettings for a source with only 1 queue for a source" in {
    val props = getProps1Queue()
    val config = JMSConfig(props)
    val settings = JMSSettings(config, false)
    val setting = settings.settings.head
    setting.source shouldBe QUEUE1
    setting.target shouldBe TOPIC1
    setting.sourceConverters shouldBe None
    setting.destinationType shouldBe QueueDestination
    setting.messageSelector shouldBe None
    settings.connectionURL shouldBe JMS_URL
  }

  "should create a JMSSettings for a source with only 1 topic for a source" in {
    val props = getProps1Topic()
    val config = JMSConfig(props)
    val settings = JMSSettings(config, false)
    val setting = settings.settings.head
    setting.source shouldBe TOPIC1
    setting.target shouldBe TOPIC1
    setting.sourceConverters shouldBe None
    setting.destinationType shouldBe TopicDestination
    setting.messageSelector shouldBe None
    settings.connectionURL shouldBe JMS_URL
  }

  "should create a JMSSettings for a source with only 1 topic with JNDI for a source" in {
    val props = getProps1TopicJNDI
    val config = JMSConfig(props)
    val settings = JMSSettings(config, false)
    val setting = settings.settings.head
    setting.source shouldBe TOPIC1
    setting.target shouldBe TOPIC1
    setting.sourceConverters shouldBe None
    setting.destinationType shouldBe TopicDestination
    setting.messageSelector shouldBe None
    settings.destinationSelector shouldBe DestinationSelector.JNDI
    settings.connectionURL shouldBe JMS_URL
  }

  "should create a JMSSettings for a source with only 1 topic, 1 queue and JNDI for a source" in {
    val props = getPropsMixJNDI()
    val config = JMSConfig(props)
    val settings = JMSSettings(config, false)
    val queue = settings.settings.head
    queue.source shouldBe QUEUE1
    queue.target shouldBe TOPIC1
    queue.sourceConverters shouldBe None
    queue.destinationType shouldBe QueueDestination
    queue.messageSelector shouldBe None

    val topic = settings.settings.last
    topic.source shouldBe TOPIC1
    topic.target shouldBe TOPIC1
    topic.sourceConverters shouldBe None
    topic.destinationType shouldBe TopicDestination
    topic.messageSelector shouldBe None

    settings.destinationSelector shouldBe DestinationSelector.JNDI
    settings.connectionURL shouldBe JMS_URL
  }

  "should create a JMSSettings for a source with only 1 topic, 1 queue and JNDI and converters for a source" in {
    val props = getPropsMixWithConverter
    val config = JMSConfig(props)
    val settings = JMSSettings(config, false)
    val queue = settings.settings.head
    queue.source shouldBe AVRO_QUEUE
    queue.target shouldBe TOPIC1
    queue.sourceConverters.get.isInstanceOf[AvroConverter] shouldBe true
    queue.destinationType shouldBe QueueDestination
    queue.messageSelector shouldBe None

    val topic = settings.settings.last
    topic.source shouldBe TOPIC1
    topic.target shouldBe TOPIC1
    topic.destinationType shouldBe TopicDestination
    topic.sourceConverters.getOrElse(None) shouldBe None
    topic.messageSelector shouldBe None

    settings.destinationSelector shouldBe DestinationSelector.CDI
    settings.connectionURL shouldBe JMS_URL
  }

  "should create a JMSSettings for a source with only 1 topic, 1 queue and JNDI and converters for a sink" in {
    val props = getPropsMixJNDIWithSink()
    val config = JMSConfig(props)
    val settings = JMSSettings(config, true)
    val queue = settings.settings.head
    queue.source shouldBe TOPIC2
    queue.target shouldBe QUEUE1
    queue.sourceConverters shouldBe None
    queue.messageSelector shouldBe None

    val topic = settings.settings.last
    topic.source shouldBe TOPIC1
    topic.target shouldBe TOPIC1
    topic.destinationType shouldBe TopicDestination
    topic.messageSelector shouldBe None

    settings.destinationSelector shouldBe DestinationSelector.CDI
    settings.connectionURL shouldBe JMS_URL
  }

  "should create a JMSSettings for a source with only 1 topic with a message selector" in {
    val props = getProps1TopicWithMessageSelector()
    val config = JMSConfig(props)
    val settings = JMSSettings(config, false)

    val topic = settings.settings.head
    topic.source shouldBe TOPIC1
    topic.target shouldBe TOPIC1
    topic.sourceConverters shouldBe None
    topic.destinationType shouldBe TopicDestination
    topic.messageSelector shouldBe Some(MESSAGE_SELECTOR)

    settings.destinationSelector shouldBe DestinationSelector.CDI
    settings.connectionURL shouldBe JMS_URL
  }

  "should throw an exception if the type is not provided" in {
    val props = getPropsTopicListIncorrect
    val config = JMSConfig(props)
    intercept[ConfigException] {
      JMSSettings(config, false)
    }
  }

  "throw an exception if the config is specifying a wrong connection factory for a sink" in {
    val props = getPropsBadFactory
    val config = JMSConfig(props)
    val settings = JMSSettings(config, true)
    intercept[javax.naming.NameNotFoundException] {
      JMSSessionProvider(settings, true)
    }
  }
}
