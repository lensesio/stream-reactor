package com.landoop.streamreactor.connect.hive.orc.vectors

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector

object BytesVectorWriter extends OrcVectorWriter[BytesColumnVector, Array[Byte]] {
  override def write(vector: BytesColumnVector, offset: Int, value: Option[Array[Byte]]): Unit = value match {
    case Some(bytes) => vector.setRef(offset, bytes, 0, bytes.length)
    case _ =>
      vector.isNull(offset) = true
      vector.noNulls        = false
  }
}
