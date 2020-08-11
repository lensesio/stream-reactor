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

import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetReader

class ParquetReaderIteratorAdaptor(parquetReader: ParquetReader[GenericRecord]) extends Iterator[GenericRecord] {

  private var currentRecord: Option[GenericRecord] = None
  private var nextRecord: Option[GenericRecord] = Option(parquetReader.read())

  override def hasNext: Boolean = {
    nextRecord.nonEmpty
  }

  override def next(): GenericRecord = {

    currentRecord = nextRecord
    nextRecord = Option(parquetReader.read())

    currentRecord.getOrElse(throw new IndexOutOfBoundsException("Unable to call next on reaching the end"))
  }
}
