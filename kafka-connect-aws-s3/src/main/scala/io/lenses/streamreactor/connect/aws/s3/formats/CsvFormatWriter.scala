
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

import au.com.bytecode.opencsv.CSVWriter
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.{PartitionNamePath, SinkData, Topic}
import io.lenses.streamreactor.connect.aws.s3.sink.extractors.ExtractorErrorAdaptor.adaptErrorResponse
import io.lenses.streamreactor.connect.aws.s3.sink.extractors.SinkDataExtractor
import io.lenses.streamreactor.connect.aws.s3.storage.S3OutputStream
import org.apache.kafka.connect.data.Schema

import java.io.OutputStreamWriter
import scala.collection.JavaConverters._
import scala.util.Try

class CsvFormatWriter(outputStreamFn: () => S3OutputStream, writeHeaders: Boolean) extends S3FormatWriter with LazyLogging {

  private val outputStream: S3OutputStream = outputStreamFn()
  private val outputStreamWriter = new OutputStreamWriter(outputStream)
  private val csvWriter = new CSVWriter(outputStreamWriter)

  private var outstandingRename: Boolean = false

  private var fieldsWritten = false

  private var fields: Array[String] = _

  override def write(keySinkData: Option[SinkData], valueSinkData: SinkData, topic: Topic): Unit = {
    if (!fieldsWritten) {
      writeFields(valueSinkData.schema().orNull)
    }
    val nextRow = fields.map(PartitionNamePath(_))
      .map(path => adaptErrorResponse(SinkDataExtractor.extractPathFromSinkData(valueSinkData)(Some(path))).orNull )
    csvWriter.writeNext(nextRow: _*)
    csvWriter.flush()
  }

  override def rolloverFileOnSchemaChange(): Boolean = true

  override def close(): Unit = {
    Try(outstandingRename = outputStream.complete)

    Try(csvWriter.flush())
    Try(outputStream.flush())
    Try(csvWriter.close())
    Try(outputStreamWriter.close())
    Try(outputStream.close())
  }

  override def getOutstandingRename: Boolean = outstandingRename

  override def getPointer: Long = outputStream.getPointer

  private def writeFields(schema: Schema): Unit = {
    fields = schema.fields().asScala.map(_.name()).toArray
    if (writeHeaders) {
      csvWriter.writeNext(fields: _*)
    }
    fieldsWritten = true
  }

}
