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
import io.lenses.streamreactor.connect.aws.s3.model.{BucketAndPath, SchemaAndValueSourceData}
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}

import scala.util.Try

class AvroFormatStreamReader(inputStreamFn: () => InputStream, bucketAndPath: BucketAndPath) extends S3FormatStreamReader[SchemaAndValueSourceData] {
  private val avroDataConverter = new AvroData(100)

  private val datumReader = new GenericDatumReader[GenericRecord]()

  private val inputStream = inputStreamFn()

  private val stream = new DataFileStream[GenericRecord](inputStream, datumReader)

  private var lineNumber: Long = -1

  override def close(): Unit = {
    Try(stream.close())
    Try(inputStream.close())
  }

  override def hasNext: Boolean = stream.hasNext

  override def next(): SchemaAndValueSourceData = {
    lineNumber += 1
    val genericRecord = stream.next
    val schemaAndValue = avroDataConverter.toConnectData(genericRecord.getSchema, genericRecord)
    SchemaAndValueSourceData(schemaAndValue, lineNumber)
  }

  override def getBucketAndPath: BucketAndPath = bucketAndPath

  override def getLineNumber: Long = lineNumber
}
