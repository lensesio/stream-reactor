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

import java.io.InputStream

import io.confluent.connect.avro.AvroData
import io.lenses.streamreactor.connect.aws.s3.formats.parquet.ParquetStreamingInputFile
import io.lenses.streamreactor.connect.aws.s3.model.{BucketAndPath, SchemaAndValueSourceData}
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader

import scala.util.Try

class ParquetFormatStreamReader(inputStreamFn: () => InputStream, fileSizeFn: () => Long, bucketAndPath: BucketAndPath)
  extends S3FormatStreamReader[SchemaAndValueSourceData] with Using {

  private val inputFile = new ParquetStreamingInputFile(inputStreamFn, fileSizeFn)
  private val avroParquetReader: ParquetReader[GenericRecord] = AvroParquetReader.builder[GenericRecord](inputFile).build()
  private val iterator = new ParquetReaderIteratorAdaptor(avroParquetReader)
  private var lineNumber: Long = -1
  private val avroDataConverter = new AvroData(100)

  override def getBucketAndPath: BucketAndPath = bucketAndPath

  override def getLineNumber: Long = lineNumber

  override def close(): Unit = Try(avroParquetReader.close())

  override def hasNext: Boolean = iterator.hasNext

  override def next(): SchemaAndValueSourceData = {

    lineNumber += 1
    val nextRec = iterator.next()

    val asConnect = avroDataConverter.toConnectData(nextRec.getSchema, nextRec)
    SchemaAndValueSourceData(asConnect, lineNumber)

  }

}
