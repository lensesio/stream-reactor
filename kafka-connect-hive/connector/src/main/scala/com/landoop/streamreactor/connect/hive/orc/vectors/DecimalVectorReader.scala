package com.landoop.streamreactor.connect.hive.orc.vectors

import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector

object DecimalVectorReader extends OrcVectorReader[DecimalColumnVector, BigDecimal] {
  override def read(offset: Int, vector: DecimalColumnVector): Option[BigDecimal] = {
    if (vector.isNull(offset)) None
    Option(vector.vector(offset).getHiveDecimal.bigDecimalValue)
  }
}
