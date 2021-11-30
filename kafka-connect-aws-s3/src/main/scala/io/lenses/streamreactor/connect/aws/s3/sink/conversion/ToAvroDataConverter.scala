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

import io.confluent.connect.avro.AvroData
import io.lenses.streamreactor.connect.aws.s3.model._
import org.apache.avro.Schema
import org.apache.kafka.connect.data.{Schema => ConnectSchema}

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters.{MapHasAsJava, SeqHasAsJava}

object ToAvroDataConverter {

  private val avroDataConverter = new AvroData(100)

  def convertSchema(connectSchema: Option[ConnectSchema]): Schema = connectSchema
    .fold(throw new IllegalArgumentException("Schema-less data is not supported for Avro/Parquet"))(avroDataConverter.fromConnectSchema)

  def convertToGenericRecord[A <: Any](sinkData: SinkData): AnyRef = {
    sinkData match {
      case StructSinkData(structVal) => avroDataConverter.fromConnectData(structVal.schema(), structVal)
      case MapSinkData(map, _) => convertMap(map)
      case ArraySinkData(array, _) => convertArray(array)
      case ByteArraySinkData(array, _) => ByteBuffer.wrap(array)
      case primitive: PrimitiveSinkData => primitive.primVal().asInstanceOf[AnyRef]
      case _: NullSinkData => null
      case other => throw new IllegalArgumentException(s"Unknown SinkData type, ${other.getClass.getSimpleName}")
    }
  }

  def convertArray(array: Seq[SinkData]): java.util.List[Any] = array.map {
    case data: PrimitiveSinkData => data.primVal()
    case StructSinkData(structVal) => structVal
    case MapSinkData(map, _) => convertMap(map)
    case ArraySinkData(iArray, _) => convertArray(iArray)
    case ByteArraySinkData(bArray, _) => ByteBuffer.wrap(bArray)
    case _ => throw new IllegalArgumentException("Complex array writing not currently supported")
  }.asJava

  def convertMap(map: Map[SinkData, SinkData]): java.util.Map[AnyRef, AnyRef] = map.map {
    case (data, data1) => convertToGenericRecord(data) -> convertToGenericRecord(data1)
  }.asJava

}
