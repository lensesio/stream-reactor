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
package io.lenses.streamreactor.connect.aws.s3.sink.conversion

import io.lenses.streamreactor.connect.aws.s3.model._

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

object ToJsonDataConverter {

  def convertArray(array: Seq[SinkData]): java.util.List[Any] = array.map {
    case data: PrimitiveSinkData => data.safeVal()
    case StructSinkData(structVal)    => structVal
    case MapSinkData(map, _)          => convertMap(map)
    case ArraySinkData(iArray, _)     => convertArray(iArray)
    case ByteArraySinkData(bArray, _) => ByteBuffer.wrap(bArray)
    case NullSinkData(_)              => null
    case _                            => throw new IllegalArgumentException("Complex array writing not currently supported")
  }.asJava

  def convertMap(map: Map[SinkData, SinkData]): java.util.Map[_, _] = map.map {
    case (data, data1) => convert(data) -> convert(data1)
  }.asJava

  def convert(data: SinkData): Any = data match {
    case data: PrimitiveSinkData => data.safeVal()
    case StructSinkData(structVal)    => structVal
    case MapSinkData(map, _)          => convertMap(map)
    case ArraySinkData(array, _)      => convertArray(array)
    case ByteArraySinkData(bArray, _) => ByteBuffer.wrap(bArray)
    case NullSinkData(_)              => null
  }

}
