
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
import io.lenses.streamreactor.connect.aws.s3.formats.parquet.ParquetOutputFile
import io.lenses.streamreactor.connect.aws.s3.storage.S3OutputStream
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.data.Struct
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.ParquetWriter.{DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE}

class ParquetFormatWriter(outputStreamFn : () => S3OutputStream) extends S3FormatWriter with LazyLogging {
  private var outstandingRename: Boolean = false

  private var outputStream: S3OutputStream = _

  private val avroDataConverter = new AvroData(100)

  private var writer: ParquetWriter[GenericRecord] = _

  override def write(struct: Struct, topic: Topic): Unit = {
    logger.debug("AvroFormatWriter - write")

    val genericRecord: GenericRecord = avroDataConverter.fromConnectData(struct.schema(), struct).asInstanceOf[GenericRecord]
    if (writer == null) {
      writer = init(genericRecord.getSchema)
    }

    writer.write(genericRecord)
    outputStream.flush()

  }

  private def init(schema: Schema): ParquetWriter[GenericRecord] = {
    outputStream = outputStreamFn()
    val outputFile = new ParquetOutputFile(outputStream)

    AvroParquetWriter
      .builder[GenericRecord](outputFile)
      .withRowGroupSize(DEFAULT_BLOCK_SIZE)
      .withPageSize(DEFAULT_PAGE_SIZE)
      .withSchema(schema)
      .build()

  }

  override def rolloverFileOnSchemaChange() = true

  override def close: Unit = {
    writer.close()
    outputStream.flush()

    outstandingRename = outputStream.complete()

    outputStream.close()
  }

  override def getOutstandingRename: Boolean = outstandingRename

  override def getPointer: Long = outputStream.getPointer()
}
