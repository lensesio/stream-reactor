package com.landoop.streamreactor.connect.hive.orc.vectors

import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector

object DoubleVectorReader extends OrcVectorReader[DoubleColumnVector, Double] {
  override def read(offset: Int, vector: DoubleColumnVector): Option[Double] = {
    if (vector.isNull(offset)) None
    else Option(vector.vector(offset))
  }
}
