/*
 * Copyright 2020 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.model

import java.io.{DataInputStream, InputStream}
import java.nio.ByteBuffer

import io.lenses.streamreactor.connect.aws.s3.config.BytesWriteMode
import io.lenses.streamreactor.connect.aws.s3.config.BytesWriteMode._

import scala.collection.mutable.ListBuffer

object BytesOutputRow {

  def apply(storedByteArray: Array[Byte], bytesWriteMode: BytesWriteMode): BytesOutputRow = {

    if (bytesWriteMode == KeyOnly) {
      new BytesOutputRow(None, None, storedByteArray, Array.empty)

    } else if (bytesWriteMode == ValueOnly) {
      new BytesOutputRow(None, None, Array.empty, storedByteArray)

    } else {
      throw new IllegalArgumentException(s"Invalid apply function for bytesWriteMode key/value only $bytesWriteMode")
    }
  }

  def apply(inputStream: InputStream, bytesWriteMode: BytesWriteMode): BytesOutputRow = {
    val dataInputStream = new DataInputStream(inputStream)

    var bytesRead: Int = 0

    if (!bytesWriteMode.entryName.toLowerCase().contains("size")) {
      throw new IllegalArgumentException(s"Invalid apply function for bytesWriteMode with size $bytesWriteMode")
    }

    val keySize: Option[Long] = if (bytesWriteMode == KeyAndValueWithSizes || bytesWriteMode == KeyWithSize) {
      bytesRead += java.lang.Long.BYTES
      Some(dataInputStream.readLong())
    } else {
      None
    }

    val valSize: Option[Long] = if (bytesWriteMode == KeyAndValueWithSizes || bytesWriteMode == ValueWithSize) {
      bytesRead += java.lang.Long.BYTES
      Some(dataInputStream.readLong())
    } else {
      None
    }

    val theKey: Array[Byte] = readSegmentFromInputStream(dataInputStream, keySize)
    bytesRead += theKey.length

    val theValue: Array[Byte] = readSegmentFromInputStream(dataInputStream, valSize)
    bytesRead += theValue.length

    BytesOutputRow(keySize, valSize, theKey, theValue, Some(bytesRead))

  }

  private def readSegmentFromInputStream(inputStream: InputStream, segmentSize: Option[Long]) = {
    segmentSize.fold(Array[Byte]()) {
      numBytes: Long =>
        val bArray = Array.ofDim[Byte](numBytes.toInt)
        inputStream.read(bArray, 0, numBytes.toInt)
        bArray
    }
  }

  def longToByteArray(l: Long): Array[Byte] = {
    val buffer = ByteBuffer.allocate(java.lang.Long.BYTES)
    buffer.putLong(l)
    val ret = buffer.array()
    require(ret.size == java.lang.Long.BYTES)
    ret
  }

}

case class BytesOutputRow(
                           keySize: Option[Long],
                           valueSize: Option[Long],
                           key: Array[Byte],
                           value: Array[Byte],
                           bytesRead: Option[Int] = None
                         ) {

  def toByteArray: Array[Byte] = {
    val buffer = new ListBuffer[Byte]()

    keySize.foreach {buffer ++= BytesOutputRow.longToByteArray(_)}
    valueSize.foreach {buffer ++= BytesOutputRow.longToByteArray(_)}

    if (key.nonEmpty) buffer ++= key
    if (value.nonEmpty) buffer ++= value
    buffer.toArray
  }

}
