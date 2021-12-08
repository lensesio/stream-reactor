package com.landoop.streamreactor.connect.hive.orc.vectors

import java.sql.Timestamp

import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector

object TimestampVectorWriter extends OrcVectorWriter[TimestampColumnVector, Timestamp] {
  override def write(vector: TimestampColumnVector, offset: Int, value: Option[Timestamp]): Unit = {
    value match {
      case Some(ts) =>
        vector.set(offset, ts)
      case _ =>
        vector.setNullValue(offset)
        vector.noNulls = false
        vector.isNull(offset) = true
    }
  }
}
