/*
 *  Copyright 2017 Datamountaineer.
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

import com.datamountaineer.connector.config.FormatType
import com.datamountaineer.streamreactor.connect.jms.JMSSinkTask
import com.datamountaineer.streamreactor.connect.jms.sink.config._
import io.confluent.common.config.ConfigException
import org.apache.activemq.ActiveMQConnectionFactory
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConversions._

class JMSSettingsTest extends WordSpec with Matchers {
  "JMSSettingsTest" should {
    "create a new instance" in {
      val config = JMSSinkConfig(
        Map(
          JMSSinkConfig.JMS_URL -> "tcp://localhost",
          JMSSinkConfig.CONNECTION_FACTORY -> classOf[ActiveMQConnectionFactory].getCanonicalName,
          JMSSinkConfig.EXPORT_ROUTE_QUERY -> "INSERT INTO mqtopic1 SELECT * FROM kafkaTopic1 WITHFORMAT AVRO;INSERT INTO mqqueue1 SELECT c1,c2 as calias FROM kafkaTopic2",
          JMSSinkConfig.TOPICS_LIST -> "mqtopic1",
          JMSSinkConfig.QUEUES_LIST -> "mqqueue1",
          JMSSinkConfig.JMS_USER -> "user1",
          JMSSinkConfig.JMS_PASSWORD -> "password1"
        ))

      val settings = JMSSettings(config)
      settings.user shouldBe Some("user1")
      settings.password shouldBe Some("password1")
      settings.connectionFactoryClass shouldBe classOf[ActiveMQConnectionFactory]
      settings.routes.size shouldBe 2

      val r1 = settings.routes.head
      r1.target shouldBe "mqtopic1"
      r1.destinationType shouldBe TopicDestination
      r1.fieldsAlias shouldBe Map.empty
      r1.source shouldBe "kafkaTopic1"
      r1.format shouldBe FormatType.AVRO

      val r2 = settings.routes.tail.head
      r2.target shouldBe "mqqueue1"
      r2.destinationType shouldBe QueueDestination
      r2.fieldsAlias shouldBe Map("c1" -> "c1", "c2" -> "calias")
      r2.source shouldBe "kafkaTopic2"
      r2.format shouldBe FormatType.JSON

    }

//    "throw an exception if the config provides routes for kafka topics not configured for the connector" in {
//      intercept[ConfigException] {
//        val config = JMSSinkConfig(
//          Map(
//            JMSSinkConfig.JMS_URL -> "tcp://localhost",
//            JMSSinkConfig.CONNECTION_FACTORY -> classOf[ActiveMQConnectionFactory].getCanonicalName,
//            JMSSinkConfig.EXPORT_ROUTE_QUERY -> "INSERT INTO mqtopic1 SELECT * FROM kafkaTopic1;INSERT INTO mqqueue1 SELECT c1,c2 as calias FROM kafkaTopic2",
//            JMSSinkConfig.TOPICS_LIST -> "mqtopic1",
//            JMSSinkConfig.QUEUES_LIST -> "mqqueue1",
//            JMSSinkConfig.MESSAGE_TYPE -> "AVRO",
//            JMSSinkConfig.JMS_USER -> "user1",
//            JMSSinkConfig.JMS_PASSWORD -> "password1"
//          ))
//
//        JMSSettings(config)
//      }
//    }

    "throw an exception if the config is missing to specify the target into the topics list" in {
      intercept[ConfigException] {
        val config = JMSSinkConfig(
          Map(
            JMSSinkConfig.JMS_URL -> "tcp://localhost",
            JMSSinkConfig.CONNECTION_FACTORY -> classOf[ActiveMQConnectionFactory].getCanonicalName,
            JMSSinkConfig.EXPORT_ROUTE_QUERY -> "INSERT INTO mqtopic1 SELECT * FROM kafkaTopic1;INSERT INTO mqqueue1 SELECT c1,c2 as calias FROM kafkaTopic2",
            JMSSinkConfig.TOPICS_LIST -> "different topic",
            JMSSinkConfig.QUEUES_LIST -> "mqqueue1",
            JMSSinkConfig.JMS_USER -> "user1",
            JMSSinkConfig.JMS_PASSWORD -> "password1"
          ))

        JMSSettings(config)
      }
    }

    "throw an exception if the config is missing to specify the target into the queues list" in {
      intercept[ConfigException] {
        val config = JMSSinkConfig(
          Map(
            JMSSinkConfig.JMS_URL -> "tcp://localhost",
            JMSSinkConfig.CONNECTION_FACTORY -> classOf[ActiveMQConnectionFactory].getCanonicalName,
            JMSSinkConfig.EXPORT_ROUTE_QUERY -> "INSERT INTO mqtopic1 SELECT * FROM kafkaTopic1 WITHFORMAT AVRO;INSERT INTO mqqueue1 SELECT c1,c2 as calias FROM kafkaTopic2",
            JMSSinkConfig.TOPICS_LIST -> "mqtopic1",
            JMSSinkConfig.QUEUES_LIST -> "mqqueueNotPresent",
            JMSSinkConfig.JMS_USER -> "user1",
            JMSSinkConfig.JMS_PASSWORD -> "password1"
          ))

        JMSSettings(config)
      }
    }

    "throw an exception if the config is specifying a wrong connection factory" in {
      intercept[ConfigException] {
        val config = JMSSinkConfig(
          Map(
            JMSSinkConfig.JMS_URL -> "tcp://localhost",
            JMSSinkConfig.CONNECTION_FACTORY -> classOf[JMSSinkTask].getCanonicalName,
            JMSSinkConfig.EXPORT_ROUTE_QUERY -> "INSERT INTO mqtopic1 SELECT * FROM kafkaTopic1;INSERT INTO mqqueue1 SELECT c1,c2 as calias FROM kafkaTopic2",
            JMSSinkConfig.TOPICS_LIST -> "mqtopic1",
            JMSSinkConfig.QUEUES_LIST -> "mqqueueNotPresent",
            JMSSinkConfig.JMS_USER -> "user1",
            JMSSinkConfig.JMS_PASSWORD -> "password1"
          ))

        JMSSettings(config)
      }
    }
  }
}
