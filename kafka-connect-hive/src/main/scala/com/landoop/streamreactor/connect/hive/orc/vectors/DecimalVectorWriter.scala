package com.landoop.streamreactor.connect.hive.orc.vectors

import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable

object DecimalVectorWriter extends OrcVectorWriter[DecimalColumnVector, BigDecimal] {
  override def write(vector: DecimalColumnVector, offset: Int, value: Option[BigDecimal]): Unit = value match {
    case None =>
      vector.isNull(offset) = true
      vector.noNulls        = false
    case Some(bd) =>
      vector.vector(offset) = new HiveDecimalWritable(HiveDecimal.create(bd.underlying()))
  }
}
