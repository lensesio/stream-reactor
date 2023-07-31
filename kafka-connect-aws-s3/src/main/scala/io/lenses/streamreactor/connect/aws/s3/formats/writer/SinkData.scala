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
package io.lenses.streamreactor.connect.aws.s3.formats.writer

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct

sealed trait SinkData {
  def schema(): Option[Schema]
  def value:    AnyRef

  def safeValue: AnyRef = value
}

sealed trait PrimitiveSinkData extends SinkData

case class BooleanSinkData(value: java.lang.Boolean, schema: Option[Schema] = None) extends PrimitiveSinkData

case class StringSinkData(value: String, schema: Option[Schema] = None) extends PrimitiveSinkData {

  /**
    * Escapes new line characters so that they don't cause line breaks in the output.  In the case of text or json mode,
    * which is line delimited, these breaks could cause the file to be read incorrectly.
    */
  override def safeValue: AnyRef = Option(value).map(_.replace("\n", "\\n")).orNull
}

case class LongSinkData(value: java.lang.Long, schema: Option[Schema] = None) extends PrimitiveSinkData

case class IntSinkData(value: java.lang.Integer, schema: Option[Schema] = None) extends PrimitiveSinkData

case class ShortSinkData(value: java.lang.Short, schema: Option[Schema] = None) extends PrimitiveSinkData

case class ByteSinkData(value: java.lang.Byte, schema: Option[Schema] = None) extends PrimitiveSinkData

case class DoubleSinkData(value: java.lang.Double, schema: Option[Schema] = None) extends PrimitiveSinkData

case class FloatSinkData(value: java.lang.Float, schema: Option[Schema] = None) extends PrimitiveSinkData

case class StructSinkData(value: Struct) extends SinkData {
  override def schema(): Option[Schema] = Option(value.schema())
}

case class MapSinkData(value: Map[SinkData, SinkData], schema: Option[Schema] = None) extends SinkData

case class ArraySinkData(value: Seq[SinkData], schema: Option[Schema] = None) extends SinkData

case class ByteArraySinkData(value: Array[Byte], schema: Option[Schema] = None) extends SinkData

case class NullSinkData(schema: Option[Schema] = None) extends SinkData {
  override def value: AnyRef = null
}
