/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.formats.reader

import io.confluent.connect.avro.AvroData
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.data.SchemaAndValue

import java.io.InputStream
import scala.util.Try

class AvroStreamReader(input: InputStream) extends CloudDataIterator[SchemaAndValue] {
  private val avroDataConverter = new AvroData(100)

  private val datumReader = new GenericDatumReader[GenericRecord]()

  private val stream = new DataFileStream[GenericRecord](input, datumReader)

  override def close(): Unit = {
    val _ = Try(stream.close())
    val _ = Try(input.close())
  }

  override def hasNext: Boolean = stream.hasNext

  override def next(): SchemaAndValue = {
    val genericRecord = stream.next
    avroDataConverter.toConnectData(genericRecord.getSchema, genericRecord)
  }
}
