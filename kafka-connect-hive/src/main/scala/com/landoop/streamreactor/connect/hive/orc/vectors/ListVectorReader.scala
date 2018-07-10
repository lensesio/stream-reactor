package com.landoop.streamreactor.connect.hive.orc.vectors

import org.apache.hadoop.hive.ql.exec.vector.{ColumnVector, ListColumnVector}

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
    val end = vector.offsets(offset).toInt + vector.lengths(offset).toInt - 1

    val elementVector = vector.child.asInstanceOf[T]

    val values = start.to(end).map { k =>
      if (allNull) None
      else if (isNulls && vector.isNull(k)) None
      else reader.read(offset, elementVector)
    }

    Option(values)
  }
}
