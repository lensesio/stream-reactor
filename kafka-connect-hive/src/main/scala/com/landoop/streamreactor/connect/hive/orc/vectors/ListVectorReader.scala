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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector

class ListVectorReader[T <: ColumnVector, U](reader: OrcVectorReader[T, U])
    extends OrcVectorReader[ListColumnVector, Seq[Option[U]]] {

  override def read(offset: Int, vector: ListColumnVector): Option[Seq[Option[U]]] = {

    // Each list is composed of a range of elements in the underlying child ColumnVector.
    // The range for list i is offsets[i]..offsets[i]+lengths[i]-1 inclusive.
    // in other words, the vector.offsets contains the start position, and then
    // the vector.lengths is used for the end position

    val isNulls = !vector.noNulls
    val repeats = vector.isRepeating
    val allNull = isNulls && repeats && vector.isNull(0)

    val start = vector.offsets(offset).toInt
    val end   = vector.offsets(offset).toInt + vector.lengths(offset).toInt - 1

    val elementVector = vector.child.asInstanceOf[T]

    val values = start.to(end).map { k =>
      if (allNull) None
      else if (isNulls && vector.isNull(k)) None
      else reader.read(offset, elementVector)
    }

    Option(values)
  }
}
