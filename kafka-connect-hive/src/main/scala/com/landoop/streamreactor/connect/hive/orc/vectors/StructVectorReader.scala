/*
 * Copyright 2017-2023 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.landoop.streamreactor.connect.hive.orc.vectors

import com.landoop.streamreactor.connect.hive.orc.OrcSchemas
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector
import org.apache.kafka.connect.data.Struct
import org.apache.orc.TypeDescription

import scala.jdk.CollectionConverters.ListHasAsScala

class StructVectorReader(readers: IndexedSeq[OrcVectorReader[_, _]], typeDescription: TypeDescription)
    extends OrcVectorReader[StructColumnVector, Struct] {

  val schema = OrcSchemas.toKafka(typeDescription)

  override def read(offset: Int, vector: StructColumnVector): Option[Struct] = {
    val struct = new Struct(schema)
    val y      = if (vector.isRepeating) 0 else offset
    typeDescription.getFieldNames.asScala.zipWithIndex.foreach {
      case (name, k) =>
        val fieldReader = readers(k).asInstanceOf[OrcVectorReader[ColumnVector, Any]]
        val fieldVector = vector.fields(k)
        val value       = fieldReader.read(y, fieldVector)
        struct.put(name, value.orNull)
    }
    Some(struct)
  }
}
