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

import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Time
import org.apache.kafka.connect.data.Timestamp
import org.apache.kafka.connect.errors.ConnectException

import java.nio.ByteBuffer
import java.util
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

object ValueToSinkDataConverter {

  def apply(value: Any, schema: Option[Schema]): SinkData = value match {
    case shortVal:  Short          => ShortSinkData(shortVal, schema.orElse(Some(Schema.OPTIONAL_INT16_SCHEMA)))
    case boolVal:   Boolean        => BooleanSinkData(boolVal, schema.orElse(Some(Schema.OPTIONAL_BOOLEAN_SCHEMA)))
    case stringVal: String         => StringSinkData(stringVal, schema.orElse(Some(Schema.OPTIONAL_STRING_SCHEMA)))
    case longVal:   Long           => LongSinkData(longVal, schema.orElse(Some(Schema.OPTIONAL_INT64_SCHEMA)))
    case intVal:    Int            => IntSinkData(intVal, schema.orElse(Some(Schema.OPTIONAL_INT32_SCHEMA)))
    case byteVal:   Byte           => ByteSinkData(byteVal, schema.orElse(Some(Schema.OPTIONAL_INT8_SCHEMA)))
    case doubleVal: Double         => DoubleSinkData(doubleVal, schema.orElse(Some(Schema.OPTIONAL_FLOAT64_SCHEMA)))
    case floatVal:  Float          => FloatSinkData(floatVal, schema.orElse(Some(Schema.OPTIONAL_FLOAT32_SCHEMA)))
    case structVal: Struct         => StructSinkData(structVal)
    case mapVal:    Map[_, _]      => MapSinkData(mapVal.asJava, schema)
    case mapVal:    util.Map[_, _] => MapSinkData(mapVal, schema)
    case bytesVal:  Array[Byte]    => ByteArraySinkData(bytesVal, schema.orElse(Some(Schema.OPTIONAL_BYTES_SCHEMA)))
    case bytesVal:  ByteBuffer     => ByteArraySinkData(bytesVal.array(), schema.orElse(Some(Schema.OPTIONAL_BYTES_SCHEMA)))
    case arrayVal:  Array[_]       => ArraySinkData(arrayVal.toList.asJava, schema)
    case listVal:   util.List[_] => ArraySinkData(listVal, schema)
    case decimal:   BigDecimal =>
      DecimalSinkData.from(decimal.bigDecimal, schema.orElse(Some(DecimalSinkData.schemaFor(decimal.bigDecimal))))
    case decimal: java.math.BigDecimal =>
      DecimalSinkData.from(decimal, schema.orElse(Some(DecimalSinkData.schemaFor(decimal))))
    case date: java.util.Date =>
      schema match {
        case Some(Date.SCHEMA)      => DateSinkData(date)
        case Some(Time.SCHEMA)      => TimeSinkData(date)
        case Some(Timestamp.SCHEMA) => TimestampSinkData(date)
        case other => throw new ConnectException(
            s"java.util.Date found but without a schema to identify type, or with unexpected schema: ${other.orNull}",
          )
      }
    case null     => NullSinkData(schema)
    case otherVal => throw new ConnectException(s"Unsupported record $otherVal:${otherVal.getClass.getCanonicalName}")
  }
}
