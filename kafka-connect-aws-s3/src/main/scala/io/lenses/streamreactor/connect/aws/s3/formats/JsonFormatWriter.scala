
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

import java.nio.charset.StandardCharsets

import io.lenses.streamreactor.connect.aws.s3.Topic
import io.lenses.streamreactor.connect.aws.s3.storage.S3OutputStream
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.json.JsonConverter

import scala.collection.JavaConverters._
import scala.util.Try

class JsonFormatWriter(outputStreamFn : () => S3OutputStream) extends S3FormatWriter {

  private val LineSeparatorBytes: Array[Byte] = System.lineSeparator.getBytes(StandardCharsets.UTF_8)

  private val outputStream: S3OutputStream = outputStreamFn()
  private val jsonConverter = new JsonConverter
  private var outstandingRename: Boolean = false

  jsonConverter.configure(
    Map("schemas.enable" -> false).asJava, false
  )

  override def write(struct: Struct, topic: Topic): Unit = {

    val dataBytes = jsonConverter.fromConnectData(topic.value, struct.schema(), struct)

    outputStream.write(dataBytes)
    outputStream.write(LineSeparatorBytes)
    outputStream.flush()
  }

  override def rolloverFileOnSchemaChange(): Boolean = false

  override def close: Unit = {
    Try (outstandingRename = outputStream.complete())

    Try (outputStream.flush())
    Try (outputStream.close())
  }

  override def getOutstandingRename: Boolean = outstandingRename

  override def getPointer: Long = outputStream.getPointer()
}
