package com.landoop.streamreactor.connect.hive.orc.vectors

import com.landoop.streamreactor.connect.hive.orc.OrcSchemas
import org.apache.hadoop.hive.ql.exec.vector.{ColumnVector, StructColumnVector}
import org.apache.kafka.connect.data.Struct
import org.apache.orc.TypeDescription


class StructVectorReader(readers: IndexedSeq[OrcVectorReader[_, _]],
                         typeDescription: TypeDescription) extends OrcVectorReader[StructColumnVector, Struct] {

  val schema = OrcSchemas.toKafka(typeDescription)

  override def read(offset: Int, vector: StructColumnVector): Option[Struct] = {
    val struct = new Struct(schema)
    val y = if (vector.isRepeating) 0 else offset
    typeDescription.getFieldNames.asScala.zipWithIndex.foreach { case (name, k) =>
      val fieldReader = readers(k).asInstanceOf[OrcVectorReader[ColumnVector, Any]]
      val fieldVector = vector.fields(k)
      val value = fieldReader.read(y, fieldVector)
      struct.put(name, value.orNull)
    }
    Some(struct)
  }
}
