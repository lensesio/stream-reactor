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
package io.lenses.streamreactor.connect.cloud.common.formats.reader

import io.confluent.connect.avro.AvroData
import io.lenses.streamreactor.connect.cloud.common.formats.reader.parquet.ParquetSeekableInputStream
import io.lenses.streamreactor.connect.cloud.common.formats.reader.parquet.ParquetStreamingInputFile
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED
import org.apache.parquet.hadoop.ParquetReader

import java.io.InputStream
import scala.util.Try

class ParquetStreamReader(
  reader: ParquetReader[GenericRecord],
) extends S3DataIterator[SchemaAndValue]
    with Using {
  private val parquetReaderIteratorAdaptor = new ParquetReaderIteratorAdaptor(reader)
  private val avroDataConverter            = new AvroData(100)

  override def close(): Unit = {
    val _ = Try(reader.close())
  }

  override def hasNext: Boolean = parquetReaderIteratorAdaptor.hasNext

  override def next(): SchemaAndValue = {
    val nextRec = parquetReaderIteratorAdaptor.next()
    avroDataConverter.toConnectData(nextRec.getSchema, nextRec)
  }

}

object ParquetStreamReader {
  def apply(
    input:     InputStream,
    size:      Long,
    recreateF: () => Either[Throwable, InputStream],
  ): ParquetStreamReader = {
    val inputFile = new ParquetStreamingInputFile(
      size,
      () =>
        new ParquetSeekableInputStream(
          input,
          () =>
            recreateF() match {
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

    new ParquetStreamReader(avroParquetReader)
  }
}
