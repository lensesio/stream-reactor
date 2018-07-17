package com.landoop.streamreactor.connect.hive.orc.vectors

import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector

object DoubleVectorWriter extends OrcVectorWriter[DoubleColumnVector, Double] {
  override def write(vector: DoubleColumnVector, offset: Int, value: Option[Double]): Unit = {
    value match {
      case Some(double) => vector.vector(offset) = double
      case _ =>
        vector.noNulls = false
        vector.isNull(offset) = true
    }
  }
}
