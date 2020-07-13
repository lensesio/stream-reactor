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

package io.lenses.streamreactor.connect.aws.s3.model

import io.lenses.streamreactor.connect.aws.s3.source.conversion.CsvToStructConverter
import org.apache.kafka.connect.data.{SchemaAndValue, Struct}

abstract class SourceData(lineNumber: Long) {
  def getLineNumber: Long = lineNumber
}

case class SchemaAndValueSourceData(
                                     data: SchemaAndValue,
                                     lineNumber: Long
                                   ) extends SourceData(lineNumber)

case class StringSourceData(
                             data: String,
                             lineNumber: Long
                           ) extends SourceData(lineNumber)

case class ByteArraySourceData(
                                data: BytesOutputRow,
                                lineNumber: Long
                              ) extends SourceData(lineNumber)

case class CsvSourceData(
                          columnHeaders: List[String],
                          data: List[String],
                          lineNumber: Long
                        ) extends SourceData(lineNumber) {
  def toStruct: Struct = CsvToStructConverter.convertToStruct(columnHeaders, data)
}
