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
package io.lenses.streamreactor.connect.jms.sink.converters

import cats.implicits._
import io.lenses.streamreactor.connect.jms.config.JMSSetting
import com.google.protobuf.DynamicMessage
import io.confluent.connect.protobuf.ProtobufData
import io.confluent.kafka.serializers.protobuf.ProtobufSchemaAndValue
import org.apache.kafka.connect.sink.SinkRecord

import java.io.IOException

case class ProtoDynamicConverter() extends ProtoConverter {
  private val protoData: ProtobufData = new ProtobufData

  override def convert(record: SinkRecord, setting: JMSSetting): Either[IOException, Array[Byte]] =
    // This is fine and will keep historic compatibility as long as all no fields are removed and new fields are added to bottom of FieldNamed schemas such as Avro.
    // This is also safe if the inbound SinkRecord schema is of protobuf form already, e.g. is instance of ProtobufSchemaAndValue
    Either.catchOnly[IOException] {
      val proto: ProtobufSchemaAndValue = protoData.fromConnectData(record.valueSchema, record.value)
      proto.getValue
        .asInstanceOf[DynamicMessage]
        .toByteArray
    }

}
