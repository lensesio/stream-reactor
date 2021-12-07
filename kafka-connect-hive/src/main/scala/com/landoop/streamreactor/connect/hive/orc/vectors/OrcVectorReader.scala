package com.landoop.streamreactor.connect.hive.orc.vectors

import org.apache.hadoop.hive.ql.exec.vector.{BytesColumnVector, ColumnVector, DoubleColumnVector, LongColumnVector}
import org.apache.orc.TypeDescription
import org.apache.orc.TypeDescription.Category


object OrcVectorReader {
  def fromSchema(schema: TypeDescription): OrcVectorReader[_ <: ColumnVector, Any] = schema.getCategory match {
    case Category.BINARY => BytesVectorReader
    case Category.BOOLEAN =>
      new MappingVectorReader[LongColumnVector, Long, Boolean](LongVectorReader)(_ == 1)
    case Category.BYTE => LongVectorReader
    case Category.DECIMAL => DecimalVectorReader
    case Category.DOUBLE => DoubleVectorReader
    case Category.FLOAT =>
      new MappingVectorReader[DoubleColumnVector, Double, Float](DoubleVectorReader)(_.toFloat)
    case Category.INT =>
      new MappingVectorReader[LongColumnVector, Long, Int](LongVectorReader)(_.toInt)
    case Category.LIST =>
      new ListVectorReader(fromSchema(schema.getChildren.get(0)))
    case Category.LONG => LongVectorReader
    case Category.SHORT => LongVectorReader
    case Category.STRING =>
      new MappingVectorReader[BytesColumnVector, Array[Byte], String](BytesVectorReader)(bytes => new String(bytes, "UTF8"))
    case Category.STRUCT =>
      val readers = schema.getChildren.asScala.map(fromSchema)
      new StructVectorReader(readers.toIndexedSeq, schema)
    case Category.TIMESTAMP => TimestampVectorReader
  }
}

trait OrcVectorReader[V <: ColumnVector, +T] {
  def read(offset: Int, vector: V): Option[T]
}

class MappingVectorReader[V <: ColumnVector, T, U](reader: OrcVectorReader[V, T])(fn: T => U) extends OrcVectorReader[V, U] {
  override def read(offset: Int, vector: V): Option[U] = reader.read(offset, vector).map(fn)
}