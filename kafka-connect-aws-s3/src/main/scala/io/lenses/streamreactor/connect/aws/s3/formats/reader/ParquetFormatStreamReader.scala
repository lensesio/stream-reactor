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
import io.lenses.streamreactor.connect.aws.s3.formats.reader.parquet.ParquetStreamingInputFile
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED
import org.apache.parquet.hadoop.ParquetReader

import java.io.InputStream
import scala.util.Try

class ParquetFormatStreamReader(
  inputStreamFn: () => InputStream,
  fileSizeFn:    () => Long,
  bucketAndPath: S3Location,
) extends S3FormatStreamReader[SchemaAndValueSourceData]
    with Using {

  private val inputFile = new ParquetStreamingInputFile(inputStreamFn, fileSizeFn)
  private val avroParquetReader: ParquetReader[GenericRecord] = {
    val conf = new Configuration
    //allow deprecated INT96 to be read as FIXED and avoid runtime exception
    conf.setBoolean(READ_INT96_AS_FIXED, true)
    AvroParquetReader.builder[GenericRecord](inputFile).withConf(conf).build()
  }
  private val parquetReaderIteratorAdaptor = new ParquetReaderIteratorAdaptor(avroParquetReader)
  private var lineNumber: Long = -1
  private val avroDataConverter = new AvroData(100)

  override def getBucketAndPath: S3Location = bucketAndPath

  override def getLineNumber: Long = lineNumber

  override def close(): Unit = {
    val _ = Try(avroParquetReader.close())
  }

  override def hasNext: Boolean = parquetReaderIteratorAdaptor.hasNext

  override def next(): SchemaAndValueSourceData = {

    lineNumber += 1
    val nextRec = parquetReaderIteratorAdaptor.next()

    val asConnect = avroDataConverter.toConnectData(nextRec.getSchema, nextRec)
    SchemaAndValueSourceData(asConnect, lineNumber)

  }

}
