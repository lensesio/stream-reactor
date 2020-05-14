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
import io.confluent.connect.avro.AvroData
import io.lenses.streamreactor.connect.aws.s3.Topic
import io.lenses.streamreactor.connect.aws.s3.storage.S3OutputStream
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.kafka.connect.data.Struct

class AvroFormatWriter(outputStreamFn : () => S3OutputStream) extends S3FormatWriter with LazyLogging {

  private val avroDataConverter = new AvroData(100)

  private var outputStream: S3OutputStream = _
  private var writer: GenericDatumWriter[GenericRecord] = _
  private var fileWriter: DataFileWriter[GenericRecord] = _
  private var outstandingRename: Boolean = false

  override def write(struct: Struct, topic: Topic): Unit = {
    logger.debug("AvroFormatWriter - write")

    val genericRecord: GenericRecord = avroDataConverter.fromConnectData(struct.schema(), struct).asInstanceOf[GenericRecord]
    if (outputStream == null || fileWriter == null) {
      init(genericRecord.getSchema)
    }
    fileWriter.append(genericRecord)
    fileWriter.flush()
  }

  private def init(schema: Schema): Unit = {
    outputStream = outputStreamFn()
    writer = new GenericDatumWriter[GenericRecord](schema)
    fileWriter = new DataFileWriter[GenericRecord](writer).create(schema, outputStream)
  }

  override def rolloverFileOnSchemaChange() = true

  override def close: Unit = {
    fileWriter.flush()

    outstandingRename = outputStream.complete()

    fileWriter.close()
    outputStream.close()

  }

  override def getOutstandingRename: Boolean = outstandingRename

  override def getPointer: Long = outputStream.getPointer()
}
