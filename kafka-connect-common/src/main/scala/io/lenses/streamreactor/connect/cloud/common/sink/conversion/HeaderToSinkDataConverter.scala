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
package io.lenses.streamreactor.connect.cloud.common.sink.conversion

import org.apache.kafka.connect.sink.SinkRecord

import scala.jdk.CollectionConverters.IterableHasAsScala

object HeaderToSinkDataConverter {

  def apply(record: SinkRecord): Map[String, SinkData] = record.headers().asScala.map(header =>
    header.key() -> ValueToSinkDataConverter(header.value(), Option(header.schema())),
  ).toMap

  def apply(record: SinkRecord, headerName: String): SinkData = {
    val header = record.headers().lastWithName(headerName)
    ValueToSinkDataConverter(header.value(), Option(header.schema()))
  }

}
