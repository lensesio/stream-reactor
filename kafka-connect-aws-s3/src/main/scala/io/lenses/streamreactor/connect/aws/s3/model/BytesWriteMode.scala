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

import java.io.DataInputStream

import enumeratum.{EnumEntry, _}
import io.lenses.streamreactor.connect.aws.s3.formats.bytes.WithSizesBytesOutputRowReader

import scala.collection.immutable

sealed trait BytesWriteMode extends EnumEntry {
  def read(storedByteArray: Array[Byte]): BytesOutputRow

  def read(inputStream: DataInputStream): BytesOutputRow
}

object BytesWriteMode extends Enum[BytesWriteMode] {
  val values: immutable.IndexedSeq[BytesWriteMode] = findValues

  case object KeyAndValueWithSizes extends BytesWriteMode {
    override def read(storedByteArray: Array[Byte]): BytesOutputRow = throw new IllegalArgumentException(s"Invalid apply function for bytesWriteMode key/value only $this")

    override def read(inputStream: DataInputStream): BytesOutputRow = WithSizesBytesOutputRowReader.read(
      inputStream,
      true,
      true
    )
  }

  case object KeyWithSize extends BytesWriteMode {
    override def read(storedByteArray: Array[Byte]): BytesOutputRow = throw new IllegalArgumentException(s"Invalid apply function for bytesWriteMode key/value only $this")

    override def read(inputStream: DataInputStream): BytesOutputRow = WithSizesBytesOutputRowReader.read(
      inputStream,
      true,
      false
    )
  }

  case object ValueWithSize extends BytesWriteMode {
    override def read(storedByteArray: Array[Byte]): BytesOutputRow = throw new IllegalArgumentException(s"Invalid apply function for bytesWriteMode key/value only $this")

    override def read(inputStream: DataInputStream): BytesOutputRow = WithSizesBytesOutputRowReader.read(
      inputStream,
      false,
      true
    )
  }

  case object KeyOnly extends BytesWriteMode {
    override def read(storedByteArray: Array[Byte]): BytesOutputRow = {
      BytesOutputRow(None, None, storedByteArray, Array.empty[Byte])
    }

    override def read(inputStream: DataInputStream): BytesOutputRow = {
      throw new IllegalArgumentException(s"Invalid apply function for bytesWriteMode with size $this")
    }
  }

  case object ValueOnly extends BytesWriteMode {
    override def read(storedByteArray: Array[Byte]): BytesOutputRow = {
      BytesOutputRow(None, None, Array.empty[Byte], storedByteArray)
    }

    override def read(inputStream: DataInputStream): BytesOutputRow = {
      throw new IllegalArgumentException(s"Invalid apply function for bytesWriteMode with size $this")
    }
  }

}
