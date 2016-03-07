package com.datamountaineer.streamreactor.connect.bloomberg

import com.bloomberglp.blpapi.{Datetime, Name, Element}
import com.bloomberglp.blpapi.Schema.Datatype
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

object MockElementFn extends MockitoSugar {
  def apply(value: Boolean, fieldName: String): Element = {
    val elem = mock[Element]
    when(elem.isNull).thenReturn(false)
    when(elem.datatype()).thenReturn(Datatype.BOOL)
    when(elem.getValueAsBool).thenReturn(value)
    when(elem.name()).thenReturn(new Name(fieldName))
    elem
  }

  def apply(value: Int, fieldName: String): Element = {
    val elem = mock[Element]
    when(elem.isNull).thenReturn(false)
    when(elem.datatype()).thenReturn(Datatype.INT32)
    when(elem.getValueAsInt32).thenReturn(value)
    when(elem.name()).thenReturn(new Name(fieldName))
    elem
  }

  def apply(value: Long, fieldName: String): Element = {
    val elem = mock[Element]
    when(elem.isNull).thenReturn(false)
    when(elem.datatype()).thenReturn(Datatype.INT64)
    when(elem.getValueAsInt64).thenReturn(value)
    when(elem.name()).thenReturn(new Name(fieldName))
    elem
  }

  def apply(value: String, fieldName: String): Element = {
    val elem = mock[Element]
    when(elem.isNull).thenReturn(false)
    when(elem.datatype()).thenReturn(Datatype.STRING)
    when(elem.getValueAsString).thenReturn(value)
    when(elem.name()).thenReturn(new Name(fieldName))
    elem
  }

  def apply(value: Double, fieldName: String): Element = {
    val elem = mock[Element]
    when(elem.isNull).thenReturn(false)
    when(elem.datatype()).thenReturn(Datatype.FLOAT64)
    when(elem.getValueAsFloat64).thenReturn(value)
    when(elem.name()).thenReturn(new Name(fieldName))
    elem
  }

  def apply(value: Float, fieldName: String): Element = {
    val elem = mock[Element]
    when(elem.isNull).thenReturn(false)
    when(elem.datatype()).thenReturn(Datatype.FLOAT32)
    when(elem.getValueAsFloat32).thenReturn(value)
    when(elem.name()).thenReturn(new Name(fieldName))
    elem
  }

  def apply(value: Datetime, fieldName: String, hasTime: Boolean): Element = {
    val elem = mock[Element]
    when(elem.isNull).thenReturn(false)
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


  def apply(elements: Seq[Element]): Element= {
    val parentElement = mock[Element]
    when(parentElement.isNull).thenReturn(false)
    when(parentElement.numElements()).thenReturn(elements.size)
    elements.zipWithIndex.foreach { case (e, i) =>
      when(parentElement.getElement(i)).thenReturn(e)
    }

    parentElement
  }
}
