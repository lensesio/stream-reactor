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
package io.lenses.streamreactor.connect.aws.s3.formats.reader

import io.confluent.connect.avro.AvroData
import io.lenses.streamreactor.connect.aws.s3.config.StreamReaderInput
import io.lenses.streamreactor.connect.aws.s3.formats.reader.parquet.ParquetSeekableInputStream
import io.lenses.streamreactor.connect.aws.s3.formats.reader.parquet.ParquetStreamingInputFile
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.source.SourceWatermark
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.connect.source.SourceRecord
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED
import org.apache.parquet.hadoop.ParquetReader

import scala.util.Try

class ParquetStreamReader(
  avroParquetReader: ParquetReader[GenericRecord],
  input:             StreamReaderInput,
) extends S3StreamReader
    with Using {
  private val parquetReaderIteratorAdaptor = new ParquetReaderIteratorAdaptor(avroParquetReader)
  private var lineNumber: Long = -1
  private val avroDataConverter = new AvroData(100)

  override def getBucketAndPath: S3Location = input.bucketAndPath

  override def getLineNumber: Long = lineNumber

  override def close(): Unit = {
    val _ = Try(avroParquetReader.close())
  }

  override def hasNext: Boolean = parquetReaderIteratorAdaptor.hasNext

  override def next(): SourceRecord = {

    lineNumber += 1
    val nextRec = parquetReaderIteratorAdaptor.next()

    val schemaAndValue = avroDataConverter.toConnectData(nextRec.getSchema, nextRec)
    new SourceRecord(
      input.sourcePartition,
      SourceWatermark.offset(input.bucketAndPath, lineNumber, input.metadata.lastModified),
      input.targetTopic.value,
      input.targetPartition,
      null,
      null,
      schemaAndValue.schema(),
      schemaAndValue.value(),
    )

  }

}

object ParquetStreamReader {
  def apply(
    input: StreamReaderInput,
  ): ParquetStreamReader = {
    val inputFile = new ParquetStreamingInputFile(
      input.metadata.size,
      () =>
        new ParquetSeekableInputStream(
          input.stream,
          () =>
            input.recreateInputStreamF() match {
              case Left(throwable) => throw throwable
              case Right(value)    => value
            },
        ),
    )
    val avroParquetReader: ParquetReader[GenericRecord] = {
      val conf = new Configuration
      //allow deprecated INT96 to be read as FIXED and avoid runtime exception
      conf.setBoolean(READ_INT96_AS_FIXED, true)
      AvroParquetReader.builder[GenericRecord](inputFile).withConf(conf).build()
    }

    new ParquetStreamReader(avroParquetReader, input)
  }
}
