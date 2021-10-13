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

package com.datamountaineer.streamreactor.connect.jms.config

import com.datamountaineer.streamreactor.connect.converters.source.AvroConverter
import com.datamountaineer.streamreactor.connect.jms
import com.datamountaineer.streamreactor.connect.jms.sink.converters.{AvroMessageConverter, ProtoMessageConverter}
import com.datamountaineer.streamreactor.connect.jms.source.converters.{CommonJMSMessageConverter, JMSStructMessageConverter}
import com.datamountaineer.streamreactor.connect.jms.{JMSSessionProvider, TestBase}
import org.apache.kafka.common.config.ConfigException
import org.scalatest.BeforeAndAfterAll

import java.util.UUID
import scala.collection.JavaConverters._
import scala.reflect.io.Path

class JMSSettingsTest extends TestBase with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    Path(AVRO_FILE).delete()
  }

  "should create a JMSSettings for a source with only 1 queue for a source" in {
    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val queueName = UUID.randomUUID().toString
    val kcql = getKCQL(kafkaTopic1, queueName, "QUEUE")
    val props = getProps(kcql, JMS_URL)
    val config = JMSConfig(props.asJava)
    val settings = JMSSettings(config, false)
    val setting = settings.settings.head
    setting.source shouldBe queueName
    setting.target shouldBe kafkaTopic1
    setting.sourceConverter.getClass shouldBe new JMSStructMessageConverter().getClass
    setting.destinationType shouldBe QueueDestination
    setting.messageSelector shouldBe None
    settings.connectionURL shouldBe JMS_URL
  }

  "should create a JMSSettings for a source with only 1 topic for a source" in {
    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val topicName = UUID.randomUUID().toString
    val kcql = getKCQL(kafkaTopic1, topicName, "TOPIC")
    val props = getProps(kcql, JMS_URL)
    val config = JMSConfig(props.asJava)
    val settings = JMSSettings(config, false)
    val setting = settings.settings.head
    setting.source shouldBe topicName
    setting.target shouldBe kafkaTopic1
    setting.sourceConverter.getClass shouldBe new JMSStructMessageConverter().getClass
    setting.destinationType shouldBe TopicDestination
    setting.messageSelector shouldBe None
    settings.connectionURL shouldBe JMS_URL
  }

  "should create a JMSSettings for a source with only 1 topic with JNDI for a source" in {
    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val topicName = UUID.randomUUID().toString
    val kcql = getKCQL(kafkaTopic1, topicName, "TOPIC")
    val props = getProps(kcql, JMS_URL) ++ Map(JMSConfigConstants.DESTINATION_SELECTOR -> DestinationSelector.JNDI.toString)
    val config = JMSConfig(props.asJava)
    val settings = JMSSettings(config, false)
    val setting = settings.settings.head
    setting.source shouldBe topicName
    setting.target shouldBe kafkaTopic1
    setting.sourceConverter.getClass shouldBe new JMSStructMessageConverter().getClass
    setting.destinationType shouldBe TopicDestination
    setting.messageSelector shouldBe None
    settings.destinationSelector shouldBe DestinationSelector.JNDI
    settings.connectionURL shouldBe JMS_URL
  }

  "should create a JMSSettings for a source with only 1 topic, 1 queue and JNDI for a source with subscriptionName" in {
    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val topicName = s"queue-${UUID.randomUUID().toString}"
    val queueName = s"topic-${UUID.randomUUID().toString}"
    val subscription = "subscriptionName"

    val kcqlQ = getKCQL(kafkaTopic1, queueName, "QUEUE")
    val kcqlT = getKCQL(kafkaTopic1, topicName, "TOPIC")
    val props = getProps(s"$kcqlQ;$kcqlT", JMS_URL) ++
      Map(JMSConfigConstants.DESTINATION_SELECTOR -> DestinationSelector.JNDI.toString) ++
      Map(JMSConfigConstants.TOPIC_SUBSCRIPTION_NAME  -> "subscriptionName")
    val config = JMSConfig(props.asJava)
    val settings = JMSSettings(config, false)
    val queue = settings.settings.head

    queue.source shouldBe queueName
    queue.target shouldBe kafkaTopic1
    queue.sourceConverter.getClass shouldBe new JMSStructMessageConverter().getClass
    queue.destinationType shouldBe QueueDestination
    queue.subscriptionName.get shouldBe subscription
    queue.messageSelector shouldBe None

    val topic = settings.settings.last
    topic.source shouldBe topicName
    topic.target shouldBe kafkaTopic1
    topic.sourceConverter.getClass shouldBe new JMSStructMessageConverter().getClass
    topic.destinationType shouldBe TopicDestination
    topic.subscriptionName.get shouldBe subscription
    topic.messageSelector shouldBe None

    settings.destinationSelector shouldBe DestinationSelector.JNDI
    settings.connectionURL shouldBe JMS_URL
  }

  "should create a JMSSettings for a source with only 1 topic, 1 queue and JNDI and converters for a source with subscription" in {
    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val topicName = UUID.randomUUID().toString
    val queueName = UUID.randomUUID().toString
    val subscription = "subscriptionName"

    val kcqlQ = getKCQLAvroSource(kafkaTopic1, queueName, "QUEUE")
    val kcqlT = getKCQLAvroSource(kafkaTopic1, topicName, "TOPIC")
    val props = getProps(s"$kcqlQ;$kcqlT", JMS_URL) ++
      Map(AvroConverter.SCHEMA_CONFIG -> getAvroProp(queueName)) ++
      Map(JMSConfigConstants.DEFAULT_SOURCE_CONVERTER_CONFIG -> "com.datamountaineer.streamreactor.connect.converters.source.AvroConverter")
    val config = JMSConfig(props.asJava)

    val settings = JMSSettings(config, false)
    val queue = settings.settings.head
    queue.source shouldBe queueName
    queue.target shouldBe kafkaTopic1
    queue.sourceConverter.getClass shouldBe new CommonJMSMessageConverter(new AvroConverter()).getClass
    queue.destinationType shouldBe QueueDestination
    queue.messageSelector shouldBe None
    queue.subscriptionName.get shouldBe subscription

    val topic = settings.settings.last
    topic.source shouldBe topicName
    topic.target shouldBe kafkaTopic1
    topic.destinationType shouldBe TopicDestination
    topic.sourceConverter.getClass shouldBe new CommonJMSMessageConverter(new AvroConverter()).getClass
    topic.messageSelector shouldBe None
    topic.subscriptionName.get shouldBe subscription

    settings.destinationSelector shouldBe DestinationSelector.CDI
    settings.connectionURL shouldBe JMS_URL
  }

  "should create a JMSSettings for a sink with only 1 topic, 1 queue and JNDI" in {
    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val topicName = UUID.randomUUID().toString
    val queueName = UUID.randomUUID().toString

    val kcqlQ = getKCQL(kafkaTopic1, queueName, "QUEUE")
    val kcqlT = getKCQL(kafkaTopic1, topicName, "TOPIC")
    val props = getProps(s"$kcqlQ;$kcqlT", JMS_URL)

    val config = JMSConfig(props.asJava)
    val settings = JMSSettings(config, true)
    val queue = settings.settings.head
    queue.source shouldBe queueName
    queue.target shouldBe kafkaTopic1
    queue.sourceConverter.getClass shouldBe new JMSStructMessageConverter().getClass
    queue.messageSelector shouldBe None

    val topic = settings.settings.last
    topic.source shouldBe topicName
    topic.target shouldBe kafkaTopic1
    topic.destinationType shouldBe TopicDestination
    topic.messageSelector shouldBe None

    settings.destinationSelector shouldBe DestinationSelector.CDI
    settings.connectionURL shouldBe JMS_URL
  }

  "should create a JMSSettings for a sink with only 1 topic, 1 queue and JNDI and converters for a sink in kcql" in {
    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val converter =  new ProtoMessageConverter().getClass
    val topicName = UUID.randomUUID().toString
    val queueName = UUID.randomUUID().toString

    val kcqlQ = getKCQLAvroSinkConverter(kafkaTopic1, queueName, "QUEUE")
    val kcqlT = getKCQLAvroSinkConverter(kafkaTopic1, topicName, "TOPIC")
    val props = getProps(s"$kcqlQ;$kcqlT", JMS_URL)

    val config = JMSConfig(props.asJava)
    val settings = JMSSettings(config, true)
    val queue = settings.settings.head
    queue.source shouldBe queueName
    queue.target shouldBe kafkaTopic1
    queue.sourceConverter.getClass shouldBe new JMSStructMessageConverter().getClass
    queue.sinkConverter.getClass shouldBe converter
    queue.messageSelector shouldBe None

    val topic = settings.settings.last
    topic.source shouldBe topicName
    topic.target shouldBe kafkaTopic1
    topic.destinationType shouldBe TopicDestination
    topic.messageSelector shouldBe None

    settings.destinationSelector shouldBe DestinationSelector.CDI
    settings.connectionURL shouldBe JMS_URL
  }

  "should create a JMSSettings for a sink with only 1 topic, 1 queue and JNDI and format for a sink in kcql" in {
    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val converter =  new ProtoMessageConverter().getClass
    val topicName = UUID.randomUUID().toString
    val queueName = UUID.randomUUID().toString

    val kcqlQ = getKCQLFormat(kafkaTopic1, queueName, "QUEUE", "PROTOBUF")
    val kcqlT = getKCQLFormat(kafkaTopic1, topicName, "TOPIC", "PROTOBUF")
    val props = getProps(s"$kcqlQ;$kcqlT", JMS_URL)
    val config = JMSConfig(props.asJava)
    val settings = JMSSettings(config, true)
    val queue = settings.settings.head
    queue.source shouldBe queueName
    queue.target shouldBe kafkaTopic1
    queue.sourceConverter.getClass shouldBe new JMSStructMessageConverter().getClass
    queue.sinkConverter.getClass shouldBe converter

    queue.messageSelector shouldBe None

    val topic = settings.settings.last
    topic.source shouldBe topicName
    topic.target shouldBe kafkaTopic1
    topic.destinationType shouldBe TopicDestination
    topic.messageSelector shouldBe None

    settings.destinationSelector shouldBe DestinationSelector.CDI
    settings.connectionURL shouldBe JMS_URL
  }

  "should create a JMSSettings for a sink with only 1 topic, 1 queue and JNDI and storeAs for a sink in kcql" in {
    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val topicName = UUID.randomUUID().toString
    val queueName = UUID.randomUUID().toString

    val kcqlQ = getKCQLStoreAsAddressedPerson(kafkaTopic1, queueName, "QUEUE")
    val kcqlT = getKCQLEmptyStoredAsNonAddressedPerson(kafkaTopic1, topicName, "TOPIC")
    val props = getProps(s"$kcqlQ;$kcqlT", JMS_URL)
    val config = JMSConfig(props.asJava)
    val settings = JMSSettings(config, true)
    val queue = settings.settings.head
    queue.source shouldBe queueName
    queue.target shouldBe kafkaTopic1
    queue.sourceConverter.getClass shouldBe new JMSStructMessageConverter().getClass
    queue.storedAs shouldBe "`datamountaineer.streamreactor.example.AddressedPerson`"
    queue.storedAsProperties.size shouldBe 0
    queue.messageSelector shouldBe None

    val topic = settings.settings.last
    topic.source shouldBe topicName
    topic.target shouldBe kafkaTopic1
    topic.destinationType shouldBe TopicDestination
    topic.messageSelector shouldBe None
    topic.storedAs shouldBe "`datamountaineer.streamreactor.example.NonAddressedPerson`"
    topic.storedAsProperties.size shouldBe 0


    settings.destinationSelector shouldBe DestinationSelector.CDI
    settings.connectionURL shouldBe JMS_URL
  }

  "should create a JMSSettings for a sink with only 1 topic, 1 queue and JNDI and converters for a sink in a connector" in {
    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val converter =  new AvroMessageConverter().getClass
    val topicName = UUID.randomUUID().toString
    val queueName = UUID.randomUUID().toString

    val kcqlQ = getKCQL(kafkaTopic1, queueName, "QUEUE")
    val kcqlT = getKCQL(kafkaTopic1, topicName, "TOPIC")
    val props = getProps(s"$kcqlQ;$kcqlT", JMS_URL) ++
      Map(JMSConfigConstants.DEFAULT_SINK_CONVERTER_CONFIG  -> "com.datamountaineer.streamreactor.connect.jms.sink.converters.AvroMessageConverter")

    val config = JMSConfig(props.asJava)
    val settings = JMSSettings(config, true)
    val queue = settings.settings.head
    queue.source shouldBe queueName
    queue.target shouldBe kafkaTopic1
    queue.sourceConverter.getClass shouldBe new JMSStructMessageConverter().getClass
    queue.sinkConverter.getClass shouldBe converter
    queue.messageSelector shouldBe None

    val topic = settings.settings.last
    topic.source shouldBe topicName
    topic.target shouldBe kafkaTopic1
    topic.destinationType shouldBe TopicDestination
    topic.messageSelector shouldBe None

    settings.destinationSelector shouldBe DestinationSelector.CDI
    settings.connectionURL shouldBe JMS_URL
  }

  "should create a JMSSettings for a source with only 1 topic with a message selector" in {
    val kafkaTopic1 = s"kafka-${UUID.randomUUID().toString}"
    val topicName = UUID.randomUUID().toString

    val kcqlT = kcqlWithMessageSelector(kafkaTopic1, topicName, MESSAGE_SELECTOR)
    val props = getProps(kcqlT, JMS_URL)
    val config = JMSConfig(props.asJava)
    val settings = JMSSettings(config, false)

    val topic = settings.settings.head
    topic.source shouldBe topicName
    topic.target shouldBe kafkaTopic1
    topic.sourceConverter.getClass shouldBe new JMSStructMessageConverter().getClass
    topic.destinationType shouldBe TopicDestination
    topic.messageSelector shouldBe Some(MESSAGE_SELECTOR)

    settings.destinationSelector shouldBe DestinationSelector.CDI
    settings.connectionURL shouldBe JMS_URL
  }

  "should throw an exception if the type is not provided" in {
    val kcql = s"INSERT INTO x SELECT * FROM y"
    val props =
      Map(JMSConfigConstants.KCQL -> kcql,
        JMSConfigConstants.JMS_USER -> JMS_USER,
        JMSConfigConstants.JMS_PASSWORD -> JMS_PASSWORD,
        JMSConfigConstants.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
        JMSConfigConstants.CONNECTION_FACTORY -> CONNECTION_FACTORY,
        JMSConfigConstants.JMS_URL -> JMS_URL,
        JMSConfigConstants.DESTINATION_SELECTOR -> DestinationSelector.JNDI.toString
      ).asJava
    val config = jms.config.JMSConfig(props)
    intercept[ConfigException] {
      JMSSettings(config, false)
    }
  }

  "throw an exception if the config is specifying a wrong connection factory for a sink" in {
    val kcql = getKCQL("topic", "queue", "QUEUE")
    val props =
      Map(JMSConfigConstants.KCQL -> kcql,
        JMSConfigConstants.JMS_USER -> JMS_USER,
        JMSConfigConstants.JMS_PASSWORD -> JMS_PASSWORD,
        JMSConfigConstants.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
        JMSConfigConstants.CONNECTION_FACTORY -> "plop",
        JMSConfigConstants.JMS_URL -> JMS_URL
      ).asJava
    val config = jms.config.JMSConfig(props)
    val settings = JMSSettings(config, true)
    intercept[javax.naming.NameNotFoundException] {
      JMSSessionProvider(settings, true)
    }
  }
}
