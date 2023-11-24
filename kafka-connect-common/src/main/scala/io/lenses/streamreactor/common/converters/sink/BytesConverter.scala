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
package io.lenses.streamreactor.common.converters.sink

import io.lenses.streamreactor.common.converters.MsgKey
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord

class BytesConverter extends Converter {
  override def convert(sinkTopic: String, data: SinkRecord): SinkRecord =
    Option(data) match {
      case None =>
        new SinkRecord(
          sinkTopic,
          0,
          null,
          null,
          Schema.BYTES_SCHEMA,
          null,
          0,
        )
      case Some(_) =>
        new SinkRecord(
          data.topic(),
          data.kafkaPartition(),
          MsgKey.schema,
          MsgKey.getStruct(sinkTopic, data.key().toString),
          Schema.BYTES_SCHEMA,
          data.value(),
          0,
        )
    }
}
