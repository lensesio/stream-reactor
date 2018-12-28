package com.landoop.streamreactor.connect.hive.orc.vectors

import org.apache.hadoop.hive.ql.exec.vector._

class StructVectorWriter(writers: Seq[OrcVectorWriter[_, _]]) extends OrcVectorWriter[StructColumnVector, Seq[_]] {

  override def write(vector: StructColumnVector, offset: Int, value: Option[Seq[_]]): Unit = {
    value match {
      case Some(seq) =>
        seq.zipWithIndex.foreach { case (v, index) =>
          val fieldWriter = writers(index).asInstanceOf[OrcVectorWriter[ColumnVector, Any]]
          val fieldVector = vector.fields(index)
          fieldWriter.write(fieldVector, offset, Option(v))
        }
      case _ =>
        vector.isNull(offset) = true
        vector.noNulls = false
    }
  }
}