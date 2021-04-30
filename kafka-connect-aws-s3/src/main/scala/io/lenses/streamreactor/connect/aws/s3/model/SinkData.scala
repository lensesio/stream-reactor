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

package io.lenses.streamreactor.connect.aws.s3.model

import org.apache.kafka.connect.data.{Schema, Struct}

case class MessageDetail(
                          keySinkData: Option[SinkData],
                          valueSinkData: SinkData,
                          headers: Map[String, SinkData]
                        )

sealed trait SinkData {
  def schema(): Option[Schema]
}

sealed trait PrimitiveSinkData extends SinkData {
  def primVal(): Any
}

case class BooleanSinkData(primVal: Boolean, schema: Option[Schema] = None) extends PrimitiveSinkData

case class StringSinkData(primVal: String, schema: Option[Schema] = None) extends PrimitiveSinkData

case class LongSinkData(primVal: Long, schema: Option[Schema] = None) extends PrimitiveSinkData

case class IntSinkData(primVal: Int, schema: Option[Schema] = None) extends PrimitiveSinkData

case class ByteSinkData(primVal: Byte, schema: Option[Schema] = None) extends PrimitiveSinkData

case class DoubleSinkData(primVal: Double, schema: Option[Schema] = None) extends PrimitiveSinkData

case class FloatSinkData(primVal: Float, schema: Option[Schema] = None) extends PrimitiveSinkData

case class StructSinkData(structVal: Struct) extends SinkData {
  override def schema(): Option[Schema] = Option(structVal.schema())
}

case class MapSinkData(map: Map[SinkData, SinkData], schema: Option[Schema] = None) extends SinkData

case class ArraySinkData(array: Seq[SinkData], schema: Option[Schema] = None) extends SinkData

case class ByteArraySinkData(array: Array[Byte], schema: Option[Schema] = None) extends SinkData

case class NullSinkData(schema: Option[Schema] = None) extends SinkData
