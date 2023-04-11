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

object OrcVectorWriter {
  def fromSchema(schema: TypeDescription): OrcVectorWriter[_ <: ColumnVector, _] =
    schema.getCategory match {
      case Category.BINARY => BytesVectorWriter
      case Category.BOOLEAN => new ContramapVectorWriter[LongColumnVector, Boolean, Long](LongVectorWriter)(b =>
          if (b) 1 else 0,
        )
      case Category.BYTE    => new ContramapVectorWriter[LongColumnVector, Byte, Long](LongVectorWriter)(b => b.toLong)
      case Category.DECIMAL => DecimalVectorWriter
      case Category.DOUBLE  => DoubleVectorWriter
      case Category.FLOAT => new ContramapVectorWriter[DoubleColumnVector, Float, Double](DoubleVectorWriter)(b =>
          b.toDouble,
        )
      case Category.INT   => new ContramapVectorWriter[LongColumnVector, Int, Long](LongVectorWriter)(b => b.toLong)
      case Category.LIST  => new ListVectorWriter(fromSchema(schema.getChildren.get(0)))
      case Category.LONG  => LongVectorWriter
      case Category.SHORT => new ContramapVectorWriter[LongColumnVector, Byte, Long](LongVectorWriter)(s => s.toLong)
      case Category.STRING =>
        new ContramapVectorWriter[BytesColumnVector, String, Array[Byte]](BytesVectorWriter)(_.getBytes("UTF-8"))
      case Category.STRUCT =>
        val writers = schema.getChildren.asScala.map(fromSchema)
        new StructVectorWriter(writers.toSeq)
      case Category.TIMESTAMP => TimestampVectorWriter
      case other              => throw new IllegalStateException(s"No match for other $other in fromSchema")
    }
}

trait OrcVectorWriter[V <: ColumnVector, T] {
  def write(vector: V, offset: Int, value: Option[T]): Unit
}

/**
  * Wraps an existing [[OrcVectorWriter]] and acts as a contramap function to that writer.
  */
class ContramapVectorWriter[V <: ColumnVector, T, U](writer: OrcVectorWriter[V, U])(fn: T => U)
    extends OrcVectorWriter[V, T] {
  override def write(vector: V, offset: Int, value: Option[T]): Unit = writer.write(vector, offset, value.map(fn))
}
