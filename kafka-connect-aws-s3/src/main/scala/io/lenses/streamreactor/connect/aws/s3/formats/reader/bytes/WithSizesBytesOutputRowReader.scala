/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.aws.s3.formats.reader.bytes

import io.lenses.streamreactor.connect.aws.s3.formats.bytes.BytesOutputRow

import java.io.DataInputStream
import java.io.InputStream

object WithSizesBytesOutputRowReader {

  def read(inputStream: DataInputStream, readKey: Boolean, readValue: Boolean): BytesOutputRow = {

    var bytesRead: Int = 0

    val keySize: Option[Long] = Option.when(readKey) {
      bytesRead += java.lang.Long.BYTES
      inputStream.readLong()
    }

    val valSize: Option[Long] = Option.when(readValue) {
      bytesRead += java.lang.Long.BYTES
      inputStream.readLong()
    }

    val theKey: Array[Byte] = readSegmentFromInputStream(inputStream, keySize)
    bytesRead += theKey.length

    val theValue: Array[Byte] = readSegmentFromInputStream(inputStream, valSize)
    bytesRead += theValue.length

    BytesOutputRow(keySize, valSize, theKey, theValue, Some(bytesRead))

  }

  private def readSegmentFromInputStream(inputStream: InputStream, segmentSize: Option[Long]) =
    segmentSize.fold(Array[Byte]()) {
      numBytes: Long =>
        val bArray = Array.ofDim[Byte](numBytes.toInt)
        inputStream.read(bArray, 0, numBytes.toInt)
        bArray
    }

}
