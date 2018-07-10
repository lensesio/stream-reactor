package com.landoop.streamreactor.connect.hive.orc.vectors

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector

object BytesVectorReader extends OrcVectorReader[BytesColumnVector, Array[Byte]] {
  override def read(offset: Int, vector: BytesColumnVector): Option[Array[Byte]] = {
    if (!vector.noNulls && vector.isNull(offset)) None else {
      Some(vector.vector.head.slice(vector.start(offset), vector.start(offset) + vector.length(offset)))
    }
  }
}