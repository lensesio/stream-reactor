package com.datamountaineer.streamreactor.connect.bloomberg

import com.bloomberglp.blpapi.Event.EventType
import com.bloomberglp.blpapi.impl.eB
import com.bloomberglp.blpapi.{Event, Message}

import scala.collection.JavaConverters._

case class MockedEvent(evType: EventType, messages: Seq[Message]) extends Event {
  override def d(): eB = throw new UnsupportedOperationException

  override def isValid: Boolean = true

  override def eventType(): EventType = evType

  override def iterator(): java.util.Iterator[Message] = {
    messages.iterator.asJava
  }
}
