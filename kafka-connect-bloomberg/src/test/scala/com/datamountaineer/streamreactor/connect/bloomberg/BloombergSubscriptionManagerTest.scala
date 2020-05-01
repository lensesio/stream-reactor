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

package com.datamountaineer.streamreactor.connect.bloomberg

import com.bloomberglp.blpapi.Event.EventType
import com.bloomberglp.blpapi._
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.JavaConverters._

class BloombergSubscriptionManagerTest extends AnyWordSpec with Matchers with MockitoSugar {
  "BloombergSubscriptionManager" should {
    "return null if there are no items in the manager buffer" in {
      val manager = new BloombergSubscriptionManager(Map(1L -> "ticker1"))
      manager.getData shouldBe None
    }

    "ignore non SUBSCRIPTION_DATA events" in {
      val manager = new BloombergSubscriptionManager(Map(1L -> "ticker1"))
      val events = Seq(EventType.ADMIN,
        EventType.AUTHORIZATION_STATUS,
        EventType.PARTIAL_RESPONSE,
        EventType.REQUEST,
        EventType.REQUEST_STATUS,
        EventType.RESOLUTION_STATUS,
        EventType.RESPONSE,
        EventType.SERVICE_STATUS,
        EventType.SESSION_STATUS,
        EventType.SUBSCRIPTION_STATUS,
        EventType.TIMEOUT,
        EventType.TOPIC_STATUS,
        EventType.TOKEN_STATUS)

      events.map { et =>
        val ev = mock[Event]
        when(ev.eventType()).thenReturn(et)
        when(ev.iterator()).thenReturn(Seq.empty[Message].iterator.asJava)
        ev
      }.foreach(manager.processEvent(_, null))
      manager.getData shouldBe None
    }

    "return all items in the buffer" in {
      val manager = new BloombergSubscriptionManager(Map(1L -> "ticker1"))

      val correlationId = new CorrelationID(1)

      val msg1 = mock[Message]
      val elem1 = MockElementFn(Seq(MockElementFn(3.15D, "FIELD1")))

      when(msg1.correlationID()).thenReturn(correlationId)
      when(msg1.asElement()).thenReturn(elem1)


      val msg2 = mock[Message]
      val elem2 = MockElementFn(Seq(MockElementFn(value = true, "FIELD2")))

      when(msg2.numElements()).thenReturn(1)
      when(msg2.correlationID()).thenReturn(correlationId)
      when(msg2.asElement()).thenReturn(elem2)

      val ev = mock[Event]
      when(ev.eventType()).thenReturn(Event.EventType.SUBSCRIPTION_DATA)
      when(ev.iterator()).thenReturn(Seq(msg1, msg2).iterator.asJava)

      manager.processEvent(ev, null)

      val data = manager.getData.get
      data.size() shouldBe 2

      data.get(0).data.size() shouldBe 2 //contains the ticker as well
      data.get(0).data.containsKey(BloombergData.SubscriptionFieldKey)
      data.get(0).data.containsKey("FIELD1") shouldBe true
      data.get(0).data.get("FIELD1") shouldBe 3.15D

      data.get(1).data.size() shouldBe 2
      data.get(1).data.containsKey(BloombergData.SubscriptionFieldKey)
      data.get(1).data.containsKey("FIELD2") shouldBe true
      data.get(1).data.get("FIELD2") shouldBe true

      manager.getData shouldBe None
    }
  }
}
