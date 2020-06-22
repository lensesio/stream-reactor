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
import io.lenses.streamreactor.connect.aws.s3.Topic
import io.lenses.streamreactor.connect.aws.s3.formats.conversion.ToAvroDataConverter
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.storage.S3OutputStream
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumWriter
import org.apache.kafka.connect.data.{Schema => ConnectSchema}
import scala.util.Try

class AvroFormatWriter(outputStreamFn: () => S3OutputStream) extends S3FormatWriter with LazyLogging {

  private var avroWriterState: Option[AvroWriterState] = None

  private var outstandingRename: Boolean = false

  override def rolloverFileOnSchemaChange() = true

  override def write(keySinkData: Option[SinkData], valueSinkData: SinkData, topic: Topic): Unit = {

    logger.debug("AvroFormatWriter - write")

    avroWriterState
      .fold {
        val newWs = new AvroWriterState(outputStreamFn(), valueSinkData.schema())
        avroWriterState = Some(newWs)
        newWs
      }(ws => ws)
      .write(valueSinkData)

  }

  override def close = {
      avroWriterState.fold(logger.debug("Requesting close when there's nothing to close"))(_.close())
  }

  override def getOutstandingRename: Boolean = outstandingRename

  override def getPointer: Long = avroWriterState.fold(0L)(_.pointer)


  class AvroWriterState(outputStream: S3OutputStream, connectSchema: Option[ConnectSchema]) {
    private val schema: Schema = ToAvroDataConverter.convertSchema(connectSchema)
    private val writer: GenericDatumWriter[AnyRef] = new GenericDatumWriter[AnyRef](schema)
    private val fileWriter: DataFileWriter[AnyRef] = new DataFileWriter[AnyRef](writer).create(schema, outputStream)

    def write(valueStruct: SinkData): Unit = {

      val genericRecord: AnyRef = ToAvroDataConverter.convertToGenericRecord(valueStruct)

      fileWriter.append(genericRecord)
      fileWriter.flush()

    }

    def close(): Unit = {
      Try(fileWriter.flush())
      Try(outstandingRename = outputStream.complete())

      Try(fileWriter.close())
      Try(outputStream.close())
    }

    def pointer(): Long = outputStream.getPointer()

  }
}
