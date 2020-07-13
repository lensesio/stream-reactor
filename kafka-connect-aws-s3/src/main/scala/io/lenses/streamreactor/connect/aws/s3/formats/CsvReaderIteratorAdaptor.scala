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

import au.com.bytecode.opencsv.CSVReader

class CsvReaderIteratorAdaptor(csvReader: CSVReader) extends Iterator[Array[String]] {

  private var currentRecord: Option[Array[String]] = None
  private var nextRecord: Option[Array[String]] = Option(csvReader.readNext())

  override def hasNext: Boolean = {
    nextRecord.nonEmpty
  }

  def peek: Option[Array[String]] = {
    hasNext
    nextRecord
  }

  override def next(): Array[String] = {

    currentRecord = nextRecord
    nextRecord = Option(csvReader.readNext())

    currentRecord.getOrElse(throw new IndexOutOfBoundsException("Unable to call next on reaching the end"))
  }
}
