package com.landoop.streamreactor.connect.hive.orc.vectors

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector

object LongVectorWriter extends OrcVectorWriter[LongColumnVector, Long] {
  override def write(vector: LongColumnVector, offset: Int, value: Option[Long]): Unit =
    value match {
      case None =>
        vector.isNull(offset) = true
        vector.noNulls        = false
      case Some(long) => vector.vector(offset) = long
    }
}
