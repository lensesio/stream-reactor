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

import com.bloomberglp.blpapi.Schema.Datatype
import com.bloomberglp.blpapi.{Datetime, Element, Name}
import org.mockito.MockitoSugar

object MockElementFn extends MockitoSugar {
  def apply(value: Boolean, fieldName: String): Element = {
    val elem = mock[Element]
    when(elem.isNull).thenReturn(false)
    when(elem.isArray).thenReturn(false)
    when(elem.datatype()).thenReturn(Datatype.BOOL)
    when(elem.getValueAsBool).thenReturn(value)
    when(elem.name()).thenReturn(new Name(fieldName))
    elem
  }

  def apply(value: Int, fieldName: String): Element = {
    val elem = mock[Element]
    when(elem.isNull).thenReturn(false)
    when(elem.isArray).thenReturn(false)
    when(elem.datatype()).thenReturn(Datatype.INT32)
    when(elem.getValueAsInt32).thenReturn(value)
    when(elem.name()).thenReturn(new Name(fieldName))
    elem
  }

  def apply(value: Long, fieldName: String): Element = {
    val elem = mock[Element]
    when(elem.isNull).thenReturn(false)
    when(elem.isArray).thenReturn(false)
    when(elem.datatype()).thenReturn(Datatype.INT64)
    when(elem.getValueAsInt64).thenReturn(value)
    when(elem.name()).thenReturn(new Name(fieldName))
    elem
  }

  def apply(value: String, fieldName: String): Element = {
    val elem = mock[Element]
    when(elem.isNull).thenReturn(false)
    when(elem.isArray).thenReturn(false)
    when(elem.datatype()).thenReturn(Datatype.STRING)
    when(elem.getValueAsString).thenReturn(value)
    when(elem.name()).thenReturn(new Name(fieldName))
    elem
  }

  def apply(value: Double, fieldName: String): Element = {
    val elem = mock[Element]
    when(elem.isNull).thenReturn(false)
    when(elem.isArray).thenReturn(false)
    when(elem.datatype()).thenReturn(Datatype.FLOAT64)
    when(elem.getValueAsFloat64).thenReturn(value)
    when(elem.name()).thenReturn(new Name(fieldName))
    elem
  }

  def apply(value: Float, fieldName: String): Element = {
    val elem = mock[Element]
    when(elem.isNull).thenReturn(false)
    when(elem.isArray).thenReturn(false)
    when(elem.datatype()).thenReturn(Datatype.FLOAT32)
    when(elem.getValueAsFloat32).thenReturn(value)
    when(elem.name()).thenReturn(new Name(fieldName))
    elem
  }

  def apply(value: Datetime, fieldName: String, hasTime: Boolean): Element = {
    val elem = mock[Element]
    when(elem.isNull).thenReturn(false)
    when(elem.isArray).thenReturn(false)
    if (hasTime) {
      when(elem.datatype()).thenReturn(Datatype.DATETIME)
      when(elem.getValueAsDatetime).thenReturn(value)
    } else {
      when(elem.datatype()).thenReturn(Datatype.DATE)
      when(elem.getValueAsDate).thenReturn(value)
    }
    when(elem.name()).thenReturn(new Name(fieldName))
    elem
  }


  def apply(elements: Seq[Element]): Element = {
    val parentElement = mock[Element]
    when(parentElement.isNull).thenReturn(false)
    when(parentElement.isArray).thenReturn(true)
    // numElements applies to SEQUENCE element, it doesn't apply to array of elements
    //when(parentElement.numElements()).thenReturn(elements.size)
    when(parentElement.numValues()).thenReturn(elements.size)
    elements.zipWithIndex.foreach { case (e, i) =>
      when(parentElement.getValueAsElement(i)).thenReturn(e)
    }
    
    parentElement
  }
}
