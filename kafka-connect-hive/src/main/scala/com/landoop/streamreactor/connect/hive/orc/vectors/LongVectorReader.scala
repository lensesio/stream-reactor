package com.landoop.streamreactor.connect.hive.orc.vectors

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector

object LongVectorReader extends OrcVectorReader[LongColumnVector, Long] {
  override def read(offset: Int, vector: LongColumnVector): Option[Long] = {
    if (vector.isNull(offset)) None
    else Option(vector.vector(offset))
  }
}
