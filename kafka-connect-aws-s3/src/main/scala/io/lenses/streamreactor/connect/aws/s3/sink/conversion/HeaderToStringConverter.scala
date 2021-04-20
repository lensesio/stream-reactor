
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

package io.lenses.streamreactor.connect.aws.s3.sink.conversion

import io.lenses.streamreactor.connect.aws.s3.model.SinkData
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.JavaConverters._

object HeaderToStringConverter {

  def apply(record: SinkRecord): Map[String, SinkData] = record.headers().asScala.map(header => header.key() -> headerValueToString(header.value(), Option(header.schema()))).toMap

  def headerValueToString(value: Any, schema: Option[Schema]): SinkData = {
    ValueToSinkDataConverter(value, schema)
  }

}
