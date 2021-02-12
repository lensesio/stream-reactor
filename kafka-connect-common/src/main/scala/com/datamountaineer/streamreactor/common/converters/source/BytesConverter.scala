/*
 *  Copyright 2017 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.datamountaineer.streamreactor.common.converters.source

import java.util.Collections

import com.datamountaineer.streamreactor.common.converters.MsgKey
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord

class BytesConverter extends Converter {
  override def convert(kafkaTopic: String,
                       sourceTopic: String,
                       messageId: String,
                       bytes: Array[Byte],
                       keys: Seq[String] = Seq.empty,
                       keyDelimiter: String = ".",
                       properties: Map[String, String] = Map.empty): SourceRecord = {
    new SourceRecord(Collections.singletonMap(Converter.TopicKey, sourceTopic),
      null,
      kafkaTopic,
      MsgKey.schema,
      MsgKey.getStruct(sourceTopic, messageId),
      Schema.BYTES_SCHEMA,
      bytes)
  }
}
