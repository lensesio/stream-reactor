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

import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct

sealed trait SinkData {
  def schema(): Option[Schema]
  def value:    Any

  def safeValue: Any = value
}

sealed trait PrimitiveSinkData extends SinkData

case class BooleanSinkData(value: Boolean, schema: Option[Schema] = None) extends PrimitiveSinkData

case class StringSinkData(value: String, schema: Option[Schema] = None) extends PrimitiveSinkData {

  /**
    * Escapes new line characters so that they don't cause line breaks in the output.  In the case of text or json mode,
    * which is line delimited, these breaks could cause the file to be read incorrectly.
    */
  override def safeValue: Any = Option(value).map(_.replace("\n", "\\n")).orNull
}

case class LongSinkData(value: Long, schema: Option[Schema] = None) extends PrimitiveSinkData

case class IntSinkData(value: Int, schema: Option[Schema] = None) extends PrimitiveSinkData

case class ShortSinkData(value: Short, schema: Option[Schema] = None) extends PrimitiveSinkData

case class ByteSinkData(value: Byte, schema: Option[Schema] = None) extends PrimitiveSinkData

case class DoubleSinkData(value: Double, schema: Option[Schema] = None) extends PrimitiveSinkData

case class FloatSinkData(value: Float, schema: Option[Schema] = None) extends PrimitiveSinkData

case class DecimalSinkData(value: Array[Byte], schema: Option[Schema]) extends PrimitiveSinkData with StrictLogging {
  override def safeValue: Any =
    (for {
      s <- schema
      v <- Option(value)
    } yield {
      Decimal.toLogical(s, v)
    }).orNull
}
object DecimalSinkData {
  val PRECISION_FIELD = "precision"

  def from(value: java.math.BigDecimal, schema: Option[Schema]): DecimalSinkData =
    Option(value).fold(DecimalSinkData(null, schema)) { _ =>
      DecimalSinkData(Decimal.fromLogical(schemaFor(value), value), schema.orElse(Some(schemaFor(value))))
    }

  def schemaFor(decimal: java.math.BigDecimal): Schema =
    SchemaBuilder.bytes.name(Decimal.LOGICAL_NAME).parameter(Decimal.SCALE_FIELD,
                                                             Integer.toString(decimal.scale()),
    ).parameter(DecimalSinkData.PRECISION_FIELD, Integer.toString(decimal.precision())).optional().version(1).build()
}

case class StructSinkData(value: Struct) extends SinkData {
  override def schema(): Option[Schema] = Option(value.schema())
}

case class MapSinkData(value: java.util.Map[_, _], schema: Option[Schema] = None) extends SinkData

case class ArraySinkData(value: java.util.List[_], schema: Option[Schema] = None) extends SinkData

case class ByteArraySinkData(value: Array[Byte], schema: Option[Schema] = None) extends SinkData

case class NullSinkData(schema: Option[Schema] = None) extends SinkData {
  override def value: Any = null
}
