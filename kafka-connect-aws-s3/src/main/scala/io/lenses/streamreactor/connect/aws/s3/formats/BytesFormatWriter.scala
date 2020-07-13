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

package io.lenses.streamreactor.connect.aws.s3.formats

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.BytesWriteMode
import io.lenses.streamreactor.connect.aws.s3.model.{ByteArraySinkData, BytesOutputRow, SinkData, Topic}
import io.lenses.streamreactor.connect.aws.s3.storage.S3OutputStream

import scala.util.Try

class BytesFormatWriter(outputStreamFn: () => S3OutputStream, bytesWriteMode: BytesWriteMode) extends S3FormatWriter with LazyLogging {

  private val outputStream: S3OutputStream = outputStreamFn()
  private var outstandingRename: Boolean = false

  override def write(keySinkData: Option[SinkData], valueSinkData: SinkData, topic: Topic): Unit = {

    val writeKeys = bytesWriteMode.entryName.contains("Key")
    val writeValues = bytesWriteMode.entryName.contains("Value")
    val writeSizes = bytesWriteMode.entryName.contains("Size")

    var byteOutputRow = BytesOutputRow(
      None,
      None,
      Array.empty,
      Array.empty
    )

    if (writeKeys) {
      keySinkData.fold(throw new IllegalArgumentException("No key supplied however requested to write key."))(keyStruct => {
        val keyDataBytes: Array[Byte] = convertToBytes(keyStruct)
        byteOutputRow = byteOutputRow.copy(
          keySize = if (writeSizes) Some(keyDataBytes.length.longValue()) else None,
          key = keyDataBytes
        )
      })
    }

    if (writeValues) {
      val valueDataBytes: Array[Byte] = convertToBytes(valueSinkData)
      byteOutputRow = byteOutputRow.copy(
        valueSize = if (writeSizes) Some(valueDataBytes.length.longValue()) else None,
        value = valueDataBytes
      )
    }

    outputStream.write(byteOutputRow.toByteArray)
    outputStream.flush()
  }

  def convertToBytes(sinkData: SinkData): Array[Byte] = {
    sinkData match {
      case ByteArraySinkData(array, _) => array
      case _ => throw new IllegalStateException("Non-binary content received.  Please check your configuration.  It may be advisable to ensure you are using org.apache.kafka.connect.converters.ByteArrayConverter\", exception)\n      case Success(value) => value")
    }
  }

  override def rolloverFileOnSchemaChange(): Boolean = false

  override def close(): Unit = {
    Try(outstandingRename = outputStream.complete)

    Try(outputStream.flush())
    Try(outputStream.close())
  }

  override def getOutstandingRename: Boolean = outstandingRename

  override def getPointer: Long = outputStream.getPointer

}
