/*
 * Copyright 2017-2024 Lenses.io Ltd
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

import io.confluent.connect.avro.AvroData
import io.confluent.connect.avro.AvroDataConfig
import io.confluent.connect.schema.AbstractDataConfig
import org.apache.avro.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.{ Schema => ConnectSchema }

import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.ZoneId
import java.time.temporal.ChronoUnit
import java.util
import java.util.Date
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.IterableHasAsJava
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

object ToAvroDataConverter {

  private val avroDataConfig = new AvroDataConfig(
    Map(
      AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG -> "true",
      AbstractDataConfig.SCHEMAS_CACHE_SIZE_CONFIG       -> "100",
    ).asJava,
  )
  private val avroDataConverter = new AvroData(avroDataConfig)

  def convertSchema(connectSchema: Option[ConnectSchema]): Schema = connectSchema
    .fold(throw new IllegalArgumentException("Schema-less data is not supported for Avro/Parquet"))(
      avroDataConverter.fromConnectSchema,
    )

  def convertToGenericRecord[A <: Any](sinkData: SinkData): Any =
    sinkData match {
      case StructSinkData(structVal)   => avroDataConverter.fromConnectData(structVal.schema(), structVal)
      case MapSinkData(map, _)         => convert(map)
      case ArraySinkData(array, _)     => convert(array)
      case ByteArraySinkData(array, _) => ByteBuffer.wrap(array)
      case primitive: PrimitiveSinkData => primitive.value
      case DateSinkData(value)      => convertDateToDaysFromEpoch(value)
      case TimeSinkData(value)      => value.getTime
      case TimestampSinkData(value) => value.toInstant.toEpochMilli
      case _: NullSinkData => null
      case other => throw new IllegalArgumentException(s"Unknown SinkData type, ${other.getClass.getSimpleName}")
    }

  private def convertDateToDaysFromEpoch[A <: Any](value: Date) =
    ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), LocalDate.ofInstant(value.toInstant, ZoneId.systemDefault()))

  private def convertArray(list: java.util.List[_]) = list.asScala.map(convert).asJava
  private def convert(value:     Any): Any =
    value match {
      case map:   java.util.Map[_, _] => convertMap(map)
      case array: Array[_]            => convertArray(array.toSeq.asJava)
      case list:  java.util.List[_]   => convertArray(list)
      case s:     Struct              => avroDataConverter.fromConnectData(s.schema(), s)
      case _ => value
    }

  private def convertMap(map: java.util.Map[_, _]): util.Map[Any, Any] =
    map.asScala.map {
      case (key, value) => convert(key) -> convert(value)
    }.asJava

}
