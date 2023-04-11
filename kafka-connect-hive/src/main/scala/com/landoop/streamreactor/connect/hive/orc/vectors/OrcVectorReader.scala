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
package com.landoop.streamreactor.connect.hive.orc.vectors

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector
import org.apache.orc.TypeDescription
import org.apache.orc.TypeDescription.Category

import scala.jdk.CollectionConverters.ListHasAsScala

object OrcVectorReader {
  def fromSchema(schema: TypeDescription): OrcVectorReader[_ <: ColumnVector, Any] = schema.getCategory match {
    case Category.BINARY => BytesVectorReader
    case Category.BOOLEAN =>
      new MappingVectorReader[LongColumnVector, Long, Boolean](LongVectorReader)(_ == 1)
    case Category.BYTE    => LongVectorReader
    case Category.DECIMAL => DecimalVectorReader
    case Category.DOUBLE  => DoubleVectorReader
    case Category.FLOAT =>
      new MappingVectorReader[DoubleColumnVector, Double, Float](DoubleVectorReader)(_.toFloat)
    case Category.INT =>
      new MappingVectorReader[LongColumnVector, Long, Int](LongVectorReader)(_.toInt)
    case Category.LIST =>
      new ListVectorReader(fromSchema(schema.getChildren.get(0)))
    case Category.LONG  => LongVectorReader
    case Category.SHORT => LongVectorReader
    case Category.STRING =>
      new MappingVectorReader[BytesColumnVector, Array[Byte], String](BytesVectorReader)(bytes =>
        new String(bytes, "UTF8"),
      )
    case Category.STRUCT =>
      val readers = schema.getChildren.asScala.map(fromSchema)
      new StructVectorReader(readers.toIndexedSeq, schema)
    case Category.TIMESTAMP => TimestampVectorReader
    case other              => throw new IllegalStateException(s"No match for other $other in fromSchema")
  }
}

trait OrcVectorReader[V <: ColumnVector, +T] {
  def read(offset: Int, vector: V): Option[T]
}

class MappingVectorReader[V <: ColumnVector, T, U](reader: OrcVectorReader[V, T])(fn: T => U)
    extends OrcVectorReader[V, U] {
  override def read(offset: Int, vector: V): Option[U] = reader.read(offset, vector).map(fn)
}
