package com.landoop.streamreactor.connect.hive.orc.vectors

import java.sql.Timestamp

import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector

object TimestampVectorReader extends OrcVectorReader[TimestampColumnVector, Timestamp] {
  override def read(offset: Int, vector: TimestampColumnVector): Option[Timestamp] = {
    if (vector.isNull(offset)) None
    else Option(new java.sql.Timestamp(vector.getTime(offset)))
  }
}
