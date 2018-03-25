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

package com.datamountaineer.streamreactor.connect.hbase

import java.nio.ByteBuffer

import org.apache.hadoop.hbase.util.Bytes

/**
  * Utility class to allow easy conversion to bytes for :Boolean, Byte, Short, Int, Long, Float, Double and  String
  */
object BytesHelper {

  /**
    * Implicit converter to sequence of bytes
    *
    * @param value - The object to convert to bytes.
    */
  implicit class ToBytesConverter(val value: Any) extends AnyVal {
    def fromBoolean(): Array[Byte] = Bytes.toBytes(value.asInstanceOf[Boolean])

    def fromByte(): Array[Byte] = Array(value.asInstanceOf[Byte])

    def fromShort(): Array[Byte] = Bytes.toBytes(value.asInstanceOf[Short])

    def fromInt(): Array[Byte] = Bytes.toBytes(value.asInstanceOf[Int])

    def fromLong(): Array[Byte] = Bytes.toBytes(value.asInstanceOf[Long])

    def fromFloat(): Array[Byte] = Bytes.toBytes(value.asInstanceOf[Float])

    def fromDouble(): Array[Byte] = Bytes.toBytes(value.asInstanceOf[Double])

    def fromString(): Array[Byte] = Bytes.toBytes(value.toString)

    def fromBytes(): Array[Byte] = {
      value match {
        case b: ByteBuffer => b.array()
        case a: Array[_] => a.asInstanceOf[Array[Byte]]
        case other => throw new IllegalArgumentException(s"Can't handle $other ")
      }
    }

    def fromBigDecimal(): Array[Byte] = Bytes.toBytes(value.asInstanceOf[java.math.BigDecimal])
  }

}
