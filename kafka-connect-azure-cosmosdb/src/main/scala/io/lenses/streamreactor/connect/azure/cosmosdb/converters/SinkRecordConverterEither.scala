/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.azure.cosmosdb.converters

import com.azure.cosmos.implementation.Document
import org.apache.kafka.connect.sink.SinkRecord
import org.json4s.JValue

import java.util
import scala.util.Try

object SinkRecordConverterEither {

  def fromMap(map: util.Map[String, AnyRef]): Either[Throwable, Document] =
    Try(SinkRecordConverter.fromMap(map)).toEither

  def fromStruct(record: SinkRecord): Either[Throwable, Document] = Try(SinkRecordConverter.fromStruct(record)).toEither

  def fromJson(record: JValue): Either[Throwable, Document] = Try(SinkRecordConverter.fromJson(record)).toEither

}
