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

import io.lenses.streamreactor.connect.aws.s3.formats.parquet.{ParquetInputFile, SeekableByteArrayInputStream}
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader

class ParquetFormatReader extends Using {
  def read(bytes: Array[Byte]): List[GenericRecord] = {

    using(new SeekableByteArrayInputStream(bytes)) {
      inputStream =>
        val inputFile = new ParquetInputFile(inputStream)

        using(AvroParquetReader
          .builder[GenericRecord](inputFile)
          .build()) {
          avroParquetReader =>
            accListFromReader(avroParquetReader, Vector())
              .toList
        }
    }
  }

  def accListFromReader[A](reader: ParquetReader[A], acc: Vector[A]): Vector[A] = {
    val current = reader.read()
    if (current == null) acc
    else accListFromReader(reader, acc :+ current)
  }
}
