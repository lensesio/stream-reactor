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

import java.io.{OutputStream, Writer}

import com.bloomberglp.blpapi.Message.Fragment
import com.bloomberglp.blpapi._

case class MockedMessage(`type`: String, correlationId: CorrelationID) extends Message {
  override def getElementAsDatetime(name: Name): Datetime = throw new UnsupportedOperationException

  override def getElementAsDatetime(s: String): Datetime = throw new UnsupportedOperationException

  override def getElementAsFloat64(name: Name): Double = throw new UnsupportedOperationException

  override def getElementAsFloat64(s: String): Double = throw new UnsupportedOperationException

  override def getElementAsInt64(name: Name): Long = throw new UnsupportedOperationException

  override def getElementAsInt64(s: String): Long = throw new UnsupportedOperationException

  override def getElementAsDate(name: Name): Datetime = throw new UnsupportedOperationException

  override def getElementAsDate(s: String): Datetime = throw new UnsupportedOperationException

  override def getElement(name: Name): Element = throw new UnsupportedOperationException

  override def getElement(s: String): Element = throw new UnsupportedOperationException

  override def getElementAsBytes(name: Name): Array[Byte] = throw new UnsupportedOperationException

  override def getElementAsBytes(s: String): Array[Byte] = throw new UnsupportedOperationException

  override def messageType(): Name = new Name(`type`)

  override def getElementAsString(name: Name): String = throw new UnsupportedOperationException

  override def getElementAsString(s: String): String = throw new UnsupportedOperationException

  override def getElementAsName(name: Name): Name = throw new UnsupportedOperationException

  override def getElementAsName(s: String): Name = throw new UnsupportedOperationException

  override def numCorrelationIds(): Int = throw new UnsupportedOperationException

  override def topicName(): String = throw new UnsupportedOperationException

  override def timeReceivedMillis(): Long = throw new UnsupportedOperationException

  override def service(): Service = throw new UnsupportedOperationException

  override def correlationIDAt(i: Int): CorrelationID = throw new UnsupportedOperationException

  override def isValid: Boolean = true

  override def numElements(): Int = throw new UnsupportedOperationException

  override def getElementAsChar(name: Name): Char = throw new UnsupportedOperationException

  override def getElementAsChar(s: String): Char = throw new UnsupportedOperationException

  override def hasElement(name: Name): Boolean = throw new UnsupportedOperationException

  override def hasElement(name: Name, b: Boolean): Boolean = throw new UnsupportedOperationException

  override def hasElement(s: String): Boolean = throw new UnsupportedOperationException

  override def hasElement(s: String, b: Boolean): Boolean = throw new UnsupportedOperationException

  override def getElementAsBool(name: Name): Boolean = throw new UnsupportedOperationException

  override def getElementAsBool(s: String): Boolean = throw new UnsupportedOperationException

  override def fragmentType(): Fragment = throw new UnsupportedOperationException

  override def correlationID(): CorrelationID = throw new UnsupportedOperationException

  override def correlationID(i: Int): CorrelationID = throw new UnsupportedOperationException

  override def getElementAsInt32(name: Name): Int = throw new UnsupportedOperationException

  override def getElementAsInt32(s: String): Int = throw new UnsupportedOperationException

  override def getElementAsFloat32(name: Name): Float = throw new UnsupportedOperationException

  override def getElementAsFloat32(s: String): Float = throw new UnsupportedOperationException

  override def asElement(): Element = throw new UnsupportedOperationException

  override def getElementAsTime(name: Name): Datetime = throw new UnsupportedOperationException

  override def getElementAsTime(s: String): Datetime = throw new UnsupportedOperationException

  override def print(outputStream: OutputStream): Unit = throw new UnsupportedOperationException

  override def print(writer: Writer): Unit = throw new UnsupportedOperationException
}
