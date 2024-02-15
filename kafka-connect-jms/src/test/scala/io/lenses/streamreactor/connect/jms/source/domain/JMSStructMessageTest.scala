/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.jms.source.domain

import org.apache.activemq.command.ActiveMQTextMessage
import org.apache.kafka.connect.data.Struct
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import javax.jms.Topic

class JMSStructMessageTest extends AnyFunSuiteLike with Matchers {

  private val TestTopicName = "MyFunkyTopicName"

  test("should handle 'reply to' and 'destination' in headers") {

    val target = "myTarget"

    val jmsDestination = new Topic {
      override def getTopicName: String = TestTopicName
    }

    val message = new ActiveMQTextMessage()
    message.setJMSReplyTo(jmsDestination)
    message.setJMSDestination(jmsDestination)
    message.setText("Some very important text")

    val sourceRecord = JMSStructMessage.getStruct(target, message)

    sourceRecord.value() match {
      case connectStruct: Struct =>
        connectStruct.get("reply_to") should be(s"topic://$TestTopicName")
        connectStruct.get("destination") should be(s"topic://$TestTopicName")
    }
  }
}
